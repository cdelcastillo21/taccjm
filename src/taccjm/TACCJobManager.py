"""
TACCJobManager Class


Note:


References:

"""

from taccjm.SSHClient2FA import SSHClient2FA  # Modified paramiko client

import os                       # OS system utility functions
import errno                    # For error messages
import tarfile                  # For sending compressed directories
import re                       # Regular Expressions
import pdb                      # Debug
import json                     # For saving and loading job configs to disk
import time                     # Time functions
import logging                  # Used to setup the Paramiko log file
import datetime                 # Date time functionality
import configparser             # For reading configs
from jinja2 import Template     # For templating input json files
import os.path                  # Path manipulation


logger = logging.getLogger(__name__)


class TACCJobManager():

    TACC_SYSTEMS = ['stampede2', 'ls5', 'frontera', 'maverick2']
    TACC_USER_PROMPT = "Username:"
    TACC_PSW_PROMPT = "Password:"
    TACC_MFA_PROMPT ="TACC Token Code:"
    TACCJM_DIR = "$SCRATCH"

    def __init__(self, system, user=None, psw=None, mfa=None, apps_dir='taccjm-apps',
            jobs_dir='taccjm-jobs'):
        """
        Create a new TACC Job Manager for jobs executed on TACC desired system
        """

        if system not in self.TACC_SYSTEMS:
            msg = f"Unrecognized TACC system {system}. Must be one of {self.TACC_SYSTEMS}."
            logger.error(msg)
            raise Exception(msg)

        self.system= f"{system}.tacc.utexas.edu"
        self.user = user

        # Connect to server
        logger.info(f"Connecting to TACC system {system}...")
        self._client = SSHClient2FA(user_prompt=self.TACC_USER_PROMPT,
                psw_prompt=self.TACC_PSW_PROMPT,
                mfa_prompt=self.TACC_MFA_PROMPT)
        self._client.load_system_host_keys()
        self._client.connect(self.system, uid=user, pswd=psw, mfa_pswd=mfa)
        logger.info(f"Succesfuly connected to {system}")

        # Set and Create jobs and apps dirs if necessary
        taccjm_path = self._execute_command(f"echo {self.TACCJM_DIR}").strip()
        self.jobs_dir = '/'.join([taccjm_path, jobs_dir])
        self.apps_dir = '/'.join([taccjm_path, apps_dir])

        logger.info("Creating if apps/jobs dirs if they don't already exist")
        self._execute_command(f"mkdir -p {self.jobs_dir}")
        self._execute_command(f"mkdir -p {self.apps_dir}")


    # TODO - Generate custom exception for failed command
    def _execute_command(self, cmnd, prnt=False):
        stdin, stdout, stderr = self._client.exec_command(cmnd)
        out = stdout.read().decode('ascii')
        err = stderr.read().decode('utf-8')

        if len(err)>0:
            raise Exception(err)

        if prnt:
            print('stdout: \n' + out)

        return out


    def showq(self):
        return self._execute_command('showq -u ' + self.user)


    def get_allocations(self):
        cmd = '/usr/local/etc/taccinfo'
        return self._execute_command(cmd)

    def list_files(self, path=TACCJM_DIR):
        cmnd = f"ls -lat {path}"
        try:
            ret = self._execute_command(cmnd)
        except Exception:
            msg = f"Unable to access path at {path}"
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)

        # Return list of files
        files = [re.split("\\s+", x)[-1] for x in re.split("\n", ret)[1:]]
        for v in ['', '.', '..']:
            if v in files:
                files.remove(v)

        # Sort and return file list
        files.sort()
        return files


    def send_file(self, local, remote, exclude_hidden=True):
        remote_fname = os.path.basename(remote)
        remote_dir = os.path.abspath(os.path.join(remote, os.pardir))
        if os.path.isdir(local):
            fname = os.path.basename(local)
            local_tar_file = f".{fname}.taccjm.tar"
            remote_tar_file = f"{remote_dir}/.taccjm_temp_{fname}.tar"
            with tarfile.open(local_tar_file, "w:gz") as tar:
                if exclude_hidden:
                    tar.add(local, arcname=remote_fname,
                      filter=lambda x : x if not os.path.basename(x.name).startswith('.') else None)
                else:
                    tar.add(local, arcname=remote_fname)

            # Send tar file
            with self._client.open_sftp() as sftp:
                sftp.put(local_tar_file, remote_tar_file)

            # Remove local tar file if sent successfully
            os.remove(local_tar_file)

            # Now untar file in destination and remove remote tar file
            untar_cmd = f"tar -xzvf {remote_tar_file} -C {remote_dir}; rm {remote_tar_file}"
            self._execute_command(untar_cmd)
        else:
            with self._client.open_sftp() as sftp:
                sftp.put(local, remote)

        # Return list of items in directory where file or directory sent should be
        return self.list_files(path=remote_dir)


    def get_file(self, remote, local):
        # TODO: If remote directory, tar file first on remote side and then transfer and unpack? 
        with self._client.open_sftp() as sftp:
            sftp.get(remote, local)


    def load_project_config(self, local_dir='.'):
        project_config_file = os.path.join(local_dir, 'project.ini')

        if not os.path.exists(project_config_file):
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), project_config_file)

        # Read project config file
        config = configparser.ConfigParser()
        config.read(project_config_file)

        return config


    def load_templated_json_file(self, path, config):
        try:
            with open(path) as file_:
                return json.loads(Template(file_.read()).render(config))
        except FileNotFoundError:
            msg = 'Unable to find json file to template ' + path
            logger.error(msg)
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT), path)


    def deploy_app(self, local_app_dir='.', app_config_path=None, overwrite=False):
        # Load project configuration file
        proj_config = self.load_project_config(local_dir=local_app_dir)

        # Load templated app configuration
        if app_config_path is None:
            app_config_path = os.path.join(local_app_dir, 'app.json')
        app_config = self.load_templated_json_file(app_config_path,  proj_config._sections)

        # Get current apps already deployed
        cur_apps = self.get_apps()

        # Version app name if necessary so different version apps can be created and deployed
        app_name = app_config['name']
        if 'version' in app_config.keys():
            app_name = f"{app_config['name']}-{app_config['version']}"

        # Only overwrite previous version of app (a new revision) if overwrite is set.
        if (app_name in cur_apps) and (not overwrite):
            msg = f"Unable to deploy app {app_name} - already exists and overwite is not set."
            logger.info(msg)
            raise Exception(msg)

        local_app_dir = os.path.join(local_app_dir, 'assets')
        remote_app_dir = '/'.join([self.apps_dir, app_name])
        self.send_file(local_app_dir, remote_app_dir)

        # Put app config in deployed app folder
        app_config_path = '/'.join([remote_app_dir, 'app.json'])
        try:
            with self._client.open_sftp() as sftp:
                with sftp.open(app_config_path, 'w') as jc:
                    json.dump(app_config, jc)
        except Exception as e:
            msg = f"Unable to save app config file to {app_config_path}. Retry deploying app."
            logger.error(msg)
            raise e

        return app_config


    def get_apps(self, head:int=-1, prnt:bool=False):
        if head>0:
            cmnd = 'ls -lat ' + self.apps_dir + '| head -' + str(head)
        else:
            cmnd = 'ls -lat ' + self.apps_dir
        #Try to access apps dir
        try:
            ret = self._execute_command(cmnd)
        except Exception:
            raise Exception('Unable to access Apps Dir ' + self.apps_dir)

        # Process otuput into list of apps available
        apps = [re.split("\\s+", x)[-1] for x in re.split("\n", ret)[1:]]
        for v in ['', '.', '..']:
            try:
                apps.remove(v)
            except ValueError:
                pass
        if prnt:
            print(ret)
        return apps


    def get_app_wrapper_script(self, appId):
        app_config = self.get_app_config(appId)
        wrapper_script = '/'.join([self.apps_dir, appId, app_config['templatePath']])
        cmnd = 'cat ' + wrapper_script
        try:
            output = self._execute_command(cmnd)
        except Exception as e:
            msg = 'App wrapper sript for app ' + app + ' not found at ' + wrapper_script 
            logger.error(msg)
            raise FileNotFoundError
        return output


    def get_jobs(self, head=-1, prnt=False):
        if head>0:
            cmnd = 'ls -lat ' + self.jobs_dir + '| head -' + str(head)
        else:
            cmnd = 'ls -lat ' + self.jobs_dir

        # Try to access jobs dir
        try:
            ret = self._execute_command(cmnd)
        except Exception as e:
            raise Exception('Unable to access Jobs Dir ' + self.jobs_dir)

        # Process otuput into list of jobs available
        jobs = [re.split("\\s+", x)[-1] for x in re.split("\n", ret)[1:]]
        for v in ['', '.', '..']:
            try:
                jobs.remove(v)
            except ValueError:
                pass
        if prnt:
            print(ret)
        return jobs


    def save_job(self, job_config):
        # If job has been setup remotely, dump this job object to it.
        try:
            job_config['ts']['dump_ts'] = datetime.datetime.fromtimestamp(
                    time.time()).strftime('%Y%m%d_%H%M%S')
            dest = job_config['job_dir'] + '/job_config.json'
            sftp = self._client.open_sftp()
            with sftp.open(dest, 'w') as jc:
                json.dump(job_config, jc)
        except Exception as e:
            msg = "Unable to write json job config file"
            logger.error(msg)
            raise e


    def load_job_config(self, job_name):
        jobs = self.get_jobs()
        if job_name not in jobs:
            raise Exeception('Job not found.')
        job_config = '/'.join([self.jobs_dir, job_name, 'job_config.json'])
        sftp = self._client.open_sftp()
        try:
            with sftp.open(job_config, 'rb') as jc:
                job_config = json.load(jc)
        except FileNotFoundError:
            msg = 'Job config for job ' + job_name + ' not found at ' + job_config
            logger.error(msg)
            raise FileNotFoundError
        sftp.close()

        return job_config

    def load_app_config(self, appId):
        # Get current apps already deployed
        cur_apps = self.get_apps()
        if appId not in cur_apps:
            msg = f"Application {job_config['appId']} does not exist."
            logger.error(msg)
            raise Exception(msg)

        # Load application config
        app_config_path = '/'.join([self.apps_dir, appId, 'app.json'])
        with self._client.open_sftp() as sftp:
            with sftp.open(app_config_path, 'rb') as jc:
                app_config = json.load(jc)

        return app_config


    def setup_job(self, local_job_dir='.', job_config_path=None):
        # Load project configuration file
        proj_config = self.load_project_config(local_dir=local_job_dir)

        # Load templated job configuration -> Default is job.json
        if job_config_path is None:
            job_config_path = os.path.join(local_job_dir, 'job.json')
        job_config = self.load_templated_json_file(job_config_path,  proj_config._sections)

        # Get app config
        app_config = self.load_app_config(job_config['appId'])

        job_config['ts'] = {'setup_ts': None,
                            'submit_ts': None,
                            'start_ts': None,
                            'end_ts': None}

        # Set timestamp when job was setup
        job_config['ts']['setup_ts'] = datetime.datetime.fromtimestamp(
                time.time()).strftime('%Y%m%d_%H%M%S')

        # Create job directory in job manager's jobs folder
        job_config['job_id'] = '{job_name}_{ts}'.format(job_name=job_config['name'],
                ts=job_config['ts']['setup_ts'])
        job_config['job_dir'] = '{job_dir}/{job_id}'.format(job_dir=self.jobs_dir,
                job_id=job_config['job_id'])
        try:
            ret = self._execute_command('mkdir ' + job_config['job_dir'])
        except Exception as e:
            msg = "Unable to setup job dir for " + job_config['job_id']
            logger.error(msg)
            raise e

        # Cleanup job directrory
        def _cleanup():
            return self._execute_command('rm -rf ' + job_config['job_dir'])

        # Copy app contents to job directory
        cmnd = 'cp -r {apps_dir}/{app}/* {job_dir}/'.format(apps_dir=self.apps_dir,
                app=job_config['appId'], job_dir=job_config['job_dir'])
        ret = self._execute_command(cmnd)

        # chmod setup script and run - Remove this since tapisv2 doesn't do?
        self._execute_command('chmod +x {job_dir}/setup.sh'.format(job_dir=job_config['job_dir']))
        cmnd = '{job_dir}/setup.sh {job_dir}'.format(job_dir=job_config['job_dir'])
        ret = self._execute_command(cmnd)

        # Load submit script template
        submit_script = """#!/bin/bash
#----------------------------------------------------
# {job_name}
# {job_desc}
# Created: {ts}
#----------------------------------------------------

#SBATCH -J {job_id}                               # Job name
#SBATCH -o {job_id}.o%j                           # Name of stdout output file
#SBATCH -e {job_id}.e%j                           # Name of stderr error file
#SBATCH -p {queue}                                # Queue (partition) name
#SBATCH -N {N}                                    # Total # of nodes
#SBATCH -n {n}                                    # Total # of mpi tasks
#SBATCH -t {rt}                                   # Run time (hh:mm:ss)"""

        # wrapper script - Load from app directory, Insert at beginning argument parsing
        try:
            wrapper_script = self.get_app_wrapper_script(job_config['appId'])
        except Exception as e:
            _cleanup()
            msg = "Couldn't get wrapper script to setup job dir for " + job_config['job_id']
            logger.error(msg)
            Exception(msg)

        # Format submit scripts with appropriate inputs for job
        submit_script = submit_script.format(job_name=job_config['name'],
                job_desc=job_config['desc'], ts=job_config['ts']['setup_ts'],
                job_id=job_config['job_id'], queue=app_config['defaultQueue'],
                N=job_config['nodeCount'], n=job_config['processorsPerNode'],
                rt=job_config['maxRunTime'])

        # submit script - add slurm directives for email and allocation if specified for job
        if job_config['email']!=None:
            submit_script += "\n#SBATCH --mail-user={email} # Email to send to".format(
                    email=job_config['email'])
            submit_script += "\n#SBATCH --mail-type=all     # Email to send to"
        # TODO: Check allocation exists? Check if we need allocation for job -> if more than one is present then we need the argument
        if job_config['allocation']!=None:
            submit_script += "\n#SBATCH -A {allocation} # Allocation name ".format(
                    allocation=job_config['allocation'])
        submit_script += "\n#----------------------------------------------------\n"
        submit_script += "\ncd {job_dir}\n".format(job_dir=job_config['job_dir'])

        # submit script - parse line to invoke wrapper.sh script that starts off application.
        execute_line = "\n{job_dir}/wrapper.sh ".format(job_dir=job_config['job_dir'])

        # Add preamble to parse arguments to wrapper script
        wrapper_preamble = "# Create start ts file\ntouch start_$(date +\"%FT%H%M%S\")\n"
        wrapper_preamble += "\n# Parse arguments passed\nfor ARGUMENT in \"$@\"\ndo\n"
        wrapper_preamble += "\n    KEY=$(echo $ARGUMENT | cut -f1 -d=)"
        wrapper_preamble += "\n    VALUE=$(echo $ARGUMENT | cut -f2 -d=)\n"
        wrapper_preamble += "\n    case \"$KEY\" in"

        # NP, the number of mpi processes available to job, is always a variable passed
        wrapper_preamble += "\n        NP)           NP=${VALUE} ;;"
        execute_line += " NP=" + str(job_config['slurm']['mpi_tasks'])

        # Transfer inputs to job directory
        for arg in job_config['inputs'].keys():
            path = job_config[arg]

            dest_path = '/'.join([job_config['job_dir'], os.path.basename(path)])
            try:
                self.send_file(path, dest_path)
            except Exception as e:
                _cleanup()
                msg = f"Unable to send input file for arg {arg['name']} to dest {dest_path}"
                logger.error(msg)
                raise Exception(msg)

            # Pass in path to input file as argument to application
            # pass name,value pair to application wrapper.sh
            execute_line += f" {arg}={dest_path}"
            wrapper_preamble += "\n        {arg})           {arg}=${{VALUE}} ;;".format(arg=arg)

        # Add on parameters passed to job
        for arg in job_config['parameters'].keys():
            value = job_config[arg]

            # pass name,value pair to application wrapper.sh
            execute_line += f" {arg}={value}"
            wrapper_preamble += f"\n        {arg})           {arg}=${{VALUE}} ;;"

        # Close off wrapper preamble
        wrapper_preamble += "\n        *)\n    esac\n\ndone\n\n"

        # submit script - add execution line to wrapper.sh
        submit_script += execute_line + '\n'

        # Line to create end ts
        wrapper_post = "\n# Create end ts file\ntouch end_$(date +\"%FT%H%M%S\")\n"

        # Write modified submit and wrapper scripts to job directory
        with self._client.open_sftp() as sftp:
            submit_dest = job_config['job_dir'] + '/submit_script.sh'
            wrapper_dest = '/'.join([job_config['job_dir'], '/wrapper.sh'])
            with sftp.open(submit_dest,  'w') as ss_file:
                ss_file.write(submit_script)
            with sftp.open(wrapper_dest, 'w') as ws_file:
                ws_file.write(wrapper_preamble + wrapper_script + wrapper_post)

        # chmod submit_scipt and wrapper script to make them executables
        try:
            self._execute_command('chmod +x ' + submit_dest)
            self._execute_command('chmod +x ' + wrapper_dest)
        except Exception as e:
            _cleanup()
            msg = "Unable to chmod wrapper or submit scripts in job dir."
            logger.error(msg)
            Exception(msg)

        # Save current job config
        try:
            self.save_job(job_config)
        except Exception as e:
            _cleanup()
            msg = "Unable to save job config after setup."
            logger.error(msg)
            raise e 

        return job_config


    def submit_job(self, job_config):
        if (job_config['ts']['setup_ts']!=None) & (job_config['ts']['submit_ts']==None):
            cmnd = 'cd {job_dir};sbatch submit_script.sh'.format(job_dir=job_config['job_dir'])
            try:
                ret = self._execute_command(cmnd)
            except Exception as e:
                msg = 'Failed to run submit job - ' + job_config['job_id']
                logger.error(msg)
                Exception(msg)
            job_config['slurm']['slurm_id'] = ret.split('\n')[-2].split(' ')[-1]
            if job_config['slurm']['slurm_id'] == 'FAILED' or job_config['slurm']['slurm_id'] == '':
                job_config['slurm']['sbatch_ret'] = ret
                raise Exception('Failed to submit SLURM Job!')
            _  = job_config['slurm'].pop('sbatch_ret', None)
            job_config['ts']['submit_ts'] = datetime.datetime.fromtimestamp(
                    time.time()).strftime('%Y%m%d_%H%M%S')
            self.save_job(job_config)
        else:
            msg = 'Job has not been initialized or has already been submitted.'
            logger.error(msg)
            raise Exception(msg)

        return job_config


    def cancel_job(self, job_config):
        if job_config['ts']['submit_ts']!=None:
            cmnd = 'scancel ' + job_config['slurm']['slurm_id']
            self._execute_command(cmnd)

            _ = job_config['slurm'].pop('slurm_id')
            job_config['ts'] = {'setup_ts': None,
                                'submit_ts': None,
                                'start_ts': None,
                                'end_ts': None}

            self.save_job(job_config)
        else:
            msg = 'Job has not been submitted yet.'
            logger.error(msg)
            raise Exception(msg)

        return job_config


    def cleanup_job(self, job_config, check=True):
        if job_config['ts']['setup_ts']!=None:
            if check and job_config['ts']['submit_ts']!=None:
                choice = input("Are you sure yo want to cancel the job? [yes/no]").lower()
                if choice != 'yes':
                    return
                self.cancel_job(job_config)
            if check:
                choice = input("Are you sure yo want to delete job directory? [yes/no]").lower()
                if choice != 'yes':
                    return

            # Remove job directory
            cmnd = 'rm -r ' + job_config['job_dir']
            self._execute_command(cmnd)
            job_config['job_dir'] = None
        else:
            msg = 'No job to cleanup. Job has not been initialized.'
            logger.error(msg)
            raise Exception(msg)

        return job_config


    def ls_job(self, job_config, path=''):
        cmnd = 'ls -lat ' + job_config['job_dir'] + '/' + path
        try:
            ret = self._execute_command(cmnd)
        except Exception as e:
            msg = 'Unable to access job path at ' + path
            logger.error(msg)
            Exception(msg)

        # Process otuput into list of jobs available
        files = [re.split("\\s+", x)[-1] for x in re.split("\n", ret)[1:]]
        for v in ['', '.', '..']:
            try:
                files.remove(v)
            except ValueError:
                pass

        return files


    def get_job_file(self, job_config, fpath, dest_dir=None):
        # Downlaod to local job dir
        local_data_dir = os.path.basename(os.path.normpath(job_config['job_dir']))
        fname = os.path.basename(os.path.normpath(fpath))
        if dest_dir!=None:
            local_data_dir = os.path.join(dest_dir, local_data_dir)

        # Make lotal data directory if it doesn't exist already
        try:
            os.mkdir(local_data_dir)
        except FileExistsError:
            logger.info("Local Job Data dir for already exists.")
        src_path = job_config['job_dir'] + '/' + fpath
        dest_path = os.path.join(local_data_dir, fname)
        try:
            self.get_file(src_path, dest_path)
        except Exception as e:
            msg = 'Unable to download job file ' + src_path + ' to destination ' + dest_path
            logger.error(msg)
            raise Exception(msg)
        return dest_path


    def send_job_file(self, job_config, fpath, dest_dir=None):
        # Get destination directory in job path to send file to
        fname = os.path.basename(os.path.normpath(fpath))
        if dest_dir!=None:
            dest_path = '/'.join([job_config['job_dir'], dest_dir, fname])
        else:
            dest_path = '/'.join([job_config['job_dir'], fname])

        try:
            self.send_file(fpath, dest_path)
        except Exception as e:
            msg = f"Unable to send file {fpath} to destination destination {dest_path}"
            logger.error(msg)
            raise Exception(msg)
        return dest_path


    def peak_job_file(self, job_config, fpath, head=-1, tail=-1, prnt=False):
        path =  job_config['job_dir'] + '/' + fpath
        if head>0:
            cmnd = 'head -' + str(head) + ' ' + path
        elif tail>0:
            cmnd = 'tail -' + str(tail) + ' ' + path
        else:
            cmnd = 'head ' + path

        try:
            ret = self._execute_command(cmnd)
        except Exception as e:
            msg = 'Unable to peak at file at ' + fpath
            logger.error(msg)
            raise Exception(msg)

        if prnt:
            print(ret)
        return ret

    def query_job(self, job_config, args={}):
        query_cmnd =  job_config['job_dir'] + '/query.sh'
        for key, val in args.items():
            query_cmnd += f" --{key}={val}"

        # Run query script
        try:
            ret = self._execute_command(query_cmnd)
        except Exception as e:
            msg = f"Error in running query script {query_cmnd} - {e}"
            logger.error(msg)
            raise Exception(msg)

        return ret

