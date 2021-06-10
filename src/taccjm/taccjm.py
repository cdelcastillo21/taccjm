import os                       # OS system utility functions
import tarfile                  # For sending compressed directories
import re                       # Regular Expressions
import pdb                      # Debug
import json                     # For saving and loading job configs to disk
import time                     # Time functions
import socket                   # This method requires that we create our own socket
import getpass                  # Allows for secure prompting and collection of the user password
import logging                  # Used to setup the Paramiko log file
import paramiko                 # Provides SSH functionality
import datetime                 # Date time functionality
import configparser             # For reading configs
from jinja2 import template     # For templating input json files

import os.path

global user, pw, mfa

logger = logging.getLogger()

def inter_handler(title, instructions, prompt_list):
    """inter_handler: the callback for paramiko.transport.auth_interactive

    The prototype for this function is defined by Paramiko, so all of the
    arguments need to be there, even though we don't use 'title' or
    'instructions'.

    The function is expected to return a tuple of data containing the
    responses to the provided prompts. Experimental results suggests that
    there will be one call of this function per prompt, but the mechanism
    allows for multiple prompts to be sent at once, so it's best to assume
    that that can happen.

    Since tuples can't really be built on the fly, the responses are
    collected in a list which is then converted to a tuple when it's time
    to return a value.

    Experiments suggest that the username prompt never happens. This makes
    sense, but the Username prompt is included here just in case.
    """
    global user, pw, mfa
    resp = []  #Initialize the response container

    #Walk the list of prompts that the server sent that we need to answer
    for pr in prompt_list:
        #str() used to to make sure that we're dealing with a string rather than a unicode string
        #strip() used to get rid of any padding spaces sent by the server

        if str(pr[0]).strip() == "Username:":
            resp.append(user)
        elif str(pr[0]).strip() == "Password:":
            resp.append(pw)
        elif str(pr[0]).strip() == "TACC Token Code:":
            resp.append(mfa)

    return tuple(resp)  #Convert the response list to a tuple and return it


class SSHClient2FA(paramiko.SSHClient):

    def __init__(self):
        """
        Create a new SSHClient.
        """
        super().__init__()
        self._user = ""
        self._pw = ""
        self._mfa = ""


    def connect(
        self,
        hostname,
        uid=None,
        pswd=None,
        mfa_pswd=None
    ):
        """
        Connect to an SSH server and authenticate to it.
        """
        global user, pw, mfa
        #Get the username, password, and MFA token code from the user
        if uid==None:
            user = input("Username: ")
        else:
            user = uid
        if pswd==None:
            pw = getpass.getpass("Password: ") #
        else:
            pw = pswd
        if mfa_pswd==None:
            mfa = input("TACC Token Code: ")
        else:
            mfa = mfa_pswd

        #Create a socket and connect it to port 22 on the host
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((hostname, 22))

        t = self._transport = paramiko.Transport(sock)

        #Tell Paramiko that the Transport is going to be used as a client
        t.start_client(timeout=10)

        #Begin authentication; note that the username and callback are passed
        t.auth_interactive(user, inter_handler)


class Stampede2Manager():

    def __init__(self, user=None, psw=None, mfa=None, apps_dir=None, jobs_dir=None):
        """
        Create a new Job Manager for jobs executed on Stampede2.
        """
        self._user = user
        self._apps_dir = apps_dir
        self._jobs_dir = jobs_dir

        # Connect to server
        logger.info("Connecting to stampede2...")
        self._client = SSHClient2FA()
        self._client.load_system_host_keys()
        self._client.connect("stampede2.tacc.utexas.edu", uid=user, pswd=psw, mfa_pswd=mfa)
        logger.info("Connecting to stampede2...")

        # Set and Create jobs and apps dirs if necessary
        if self._jobs_dir==None:
            self._jobs_dir = '/'.join([self._execute_command('echo $SCRATCH').strip(), 'taccjm-jobs'])
        if self._apps_dir==None:
            self._apps_dir = '/'.join([self._execute_command('echo $WORK').strip(), 'taccjm-apps'])

        logger.info("Creating if apps/jobs dirs if they don't already exist")
        self._mkdir(self._jobs_dir)
        self._mkdir(self._apps_dir)


    def _mkdir(self, remote_path):
        sftp = self._client.open_sftp()
        try:
            sftp.chdir(remote_path)  # Test if remote_path exists
        except FileNotFoundError:
            sftp.mkdir(remote_path)  # Create remote_path
        sftp.close()



    # TODO - Generate custom exception for failed command
    def _execute_command(self, cmnd, prnt=False):
        stdin, stdout, stderr = self._client.exec_command(cmnd)
        out = stdout.read().decode('ascii')
        err = stdout.read().decode('utf-8')

        if len(err)>0:
            raise Exception(err)

        if prnt:
            print('stdout: \n' + out)

        return out


    def showq(self):
        return self._execute_command('showq -u ' + user)


    def get_allocations(self):
        cmd = '/usr/local/etc/taccinfo'
        return self._execute_command(cmd)


    def send_file(self, local, remote, isdir=False, exclude_hidden=True):
        sftp = self._client.open_sftp()
        if isdir:
            fname = os.path.basename(local)
            remote_fname = os.path.basename(remote)
            remote_dir = os.path.abspath(os.path.join(remote, os.pardir))
            tmp_remote_dir = remote_dir + '/.taccjm_tmp'
            self._mkdir(tmp_remote_dir)

            local_tar_file = f".{fname}.taccjm.tar"
            remote_tar_file = f"{tmp_remote_dir}/.taccjm.temp.{fname}.tar"
            with tarfile.open(local_tar_file, "w:gz") as tar:
                if exclude_hidden:
                    tar.add(local, arcname=fname,
                      filter=lambda x : x if not os.path.basename(x.name).startswith('.') else None)
                else:
                    tar.add(local, arcname=fname)
            sftp.put(local_tar_file, remote_tar_file)

            # Remove local tar file if sent successfully
            os.remove(local_tar_file)

            # Now untar file in destination and remove remote tar file
            untar_cmd = f"tar -xzvf {remote_tar_file} -C {tmp_remote_dir}; rm {remote_tar_file}"
            self._execute_command(untar_cmd)

            # Move to desired destination and remove tmpdir
            remote = remote[:-1] if remote[-1]=='/' else remote
            self._execute_command(f"mv {tmp_remote_dir}/{fname} {remote}; rm -rf {tmp_remote_dir}")
        else:
            sftp = self._client.open_sftp()
            sftp.put(local, remote)
            sftp.close()


    def get_file(self, remote, local):
        sftp = self._client.open_sftp()
        sftp.get(remote, local)
        sftp.close()


    def deploy_app(self, app_name, local_app_dir, version=None, overwrite=False):
        cur_apps = self.get_apps()
        if version!=None:
            app_name = app_name + f"-v{version}"
        if (app_name in cur_apps) and (not overwrite):
            msg = f"Unable to deploy app {app_name} - already exists and overwite is not set."
            logger.info(msg)
            raise Exception(msg)
        else:
            remote_app_dir = '/'.join([self._apps_dir, app_name])
            self.send_file(local_app_dir, remote_app_dir, isdir=True)

        if (app_name in cur_apps) and overwrite:
            cmd = f"cp {remote_app_dir}/{app_name}/* {remote_app_dir}/; rm -rf {remote_app_dir}/{app_name}"
            self._execute_command(cmd)



    def get_apps(self, head:int=-1, prnt:bool=False):
        if head>0:
            cmnd = 'ls -lat ' + self._apps_dir + '| head -' + str(head)
        else:
            cmnd = 'ls -lat ' + self._apps_dir

        # Try to access apps dir
        try:
            ret = self._execute_command(cmnd)
        except Exception:
            raise Exception('Unable to access Apps Dir ' + self._apps_dir)

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


    def get_app_wrapper_script(self, app):
        wrapper_script = '/'.join([self._apps_dir, app, 'wrapper.sh'])
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
            cmnd = 'ls -lat ' + self._jobs_dir + '| head -' + str(head)
        else:
            cmnd = 'ls -lat ' + self._jobs_dir

        # Try to access jobs dir
        try:
            ret = self._execute_command(cmnd)
        except Exception as e:
            raise Exception('Unable to access Jobs Dir ' + self._jobs_dir)

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
        job_config = '/'.join([self._jobs_dir, job_name, 'job_config.json'])
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


    def setup_job(self, job_config):
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
        job_config['job_dir'] = '{job_dir}/{job_id}'.format(job_dir=self._jobs_dir,
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
        cmnd = 'cp -r {apps_dir}/{app}/* {job_dir}/'.format(apps_dir=self._apps_dir,
                app=job_config['app'], job_dir=job_config['job_dir'])
        ret = self._execute_command(cmnd)

        # chmod setup script and run
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
            wrapper_script = self.get_app_wrapper_script(job_config['app'])
        except Exception as e:
            _cleanup()
            msg = "Couldn't get wrapper script to setup job dir for " + job_config['job_id']
            logger.error(msg)
            Exception(msg)

        # Format submit scripts with appropriate inputs for job
        submit_script = submit_script.format(job_name=job_config['name'],
                job_desc=job_config['desc'], ts=job_config['ts']['setup_ts'],
                job_id=job_config['job_id'], queue=job_config['slurm']['queue'],
                N=job_config['slurm']['nodes'], n=job_config['slurm']['mpi_tasks'],
                rt=job_config['slurm']['max_runtime'])

        # submit script - add slurm directives for email and allocation if specified for job
        if job_config['slurm']['email']!=None:
            submit_script += "\n#SBATCH --mail-user={email} # Email to send to".format(
                    email=job_config['slurm']['email'])
            submit_script += "\n#SBATCH --mail-type=all     # Email to send to"
        if job_config['slurm']['allocation']!=None:
            submit_script += "\n#SBATCH -A {allocation} # Allocation name ".format(
                    allocation=job_config['slurm']['allocation'])
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
        for arg in job_config['args']:
            value = arg['value']
            is_file = arg.copy().pop('is_file', False)
            is_dir = arg.copy().pop('is_dir', False)
            if is_file or is_dir:
                # If local file, then transfer file over to execution system
                dest_dir = arg.copy().pop('dest_dir', None)
                if dest_dir==None:
                    dest = job_config['job_dir'] + '/' + os.path.basename(arg['value'])
                else:
                    dest_dir = ('/' + dest_dir) if dest_dir[0]!='/' else dest_dir
                    dest_dir = (dest_dir + '/') if dest_dir[-1]!='/' else dest_dir
                    dest = job_config['job_dir'] + dest_dir + os.path.basename(arg['value'])
                try:
                    self.send_file(arg['value'], dest, isdir=is_dir)
                except Exception as e:
                    _cleanup()
                    msg = "Unable to send file for arg " + arg['name'] + " to dest " + dest
                    logger.error(msg)
                    raise Exception(msg)
                # Change arg value to file path on execution system
                value  = dest
            if arg['is_arg']:
                # If is an actual argument, and not just data we are transferring over,
                # pass name,value pair to application wrapper.sh
                execute_line += " " + arg['name'] + "=" + str(value)
                wrapper_preamble += "\n        {arg})           {arg}=${{VALUE}} ;;".format(arg=arg['name'])

        wrapper_preamble += "\n        *)\n    esac\n\ndone\n\n"

        # submit script - add execution line to wrapper.sh
        submit_script += execute_line + '\n'

        # Line to create end ts
        wrapper_post = "\n# Create end ts file\ntouch end_$(date +\"%FT%H%M%S\")\n"

        # Write modified submit and wrapper scripts to job directory
        sftp = self._client.open_sftp()
        submit_dest = job_config['job_dir'] + '/submit_script.sh'
        wrapper_dest = job_config['job_dir'] + '/wrapper.sh'
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

