import os
import stat
import subprocess
import sys
from typing import Optional


class Executable(object):

    def __init__(self, output_file, *commands):
        self._base_output_file = output_file
        self._output_file = output_file
        self._shebang = '/usr/bin/env bash'
        self._commands = list(commands)

    def attach_output_dir(self, output_dir: str):
        self._output_file = os.path.join(output_dir, self._output_file)

    def install(self):
        self.create_file()

    def create_file(self):
        if os.path.isfile(self._output_file):
            print(f'{self._output_file} exists, skipping {self.get_name()}')
            return

        with open(self._output_file, 'w') as out:
            out.write(f'#!{self._shebang}\n')
            out.write('\n'.join(self._commands))
            out.write('\n')
        os.chmod(self._output_file, stat.S_IRWXU | stat.S_IXGRP | stat.S_IXOTH)

    def get_name(self, file: Optional[str] = None):
        if not file:
            file = self._output_file
        name, ext = os.path.splitext(os.path.basename(file))
        return name


class PythonExecutable(Executable):

    def __init__(self, python_script, *dependencies):
        self._python_executable = PythonExecutable.find_system_executable()
        self._python_script = python_script
        self._dependencies = dependencies

        super().__init__(self.get_name(python_script))

    @classmethod
    def find_system_executable(cls) -> Optional[str]:
        if hasattr(cls, '_location'):
            return cls._location

        from shutil import which

        v = sys.version_info
        executables = ['python']
        executables.append(f'{executables[0]}{v.major}')
        executables.append(f'{executables[1]}.{v.minor}')
        executables.append(f'{executables[2]}.{v.micro}')

        found_executable = None
        for executable in executables[::-1]:
            found_executable = which(executable)
            if found_executable:
                break
        cls._location = found_executable
        return cls._location

    def run_python_command(self, *args, python_exec: Optional[
        str] = None) -> subprocess.CompletedProcess:
        if not python_exec:
            python_exec = self._python_executable
        return subprocess.run([
            python_exec, '-m', *args])

    def get_virtual_env_dir(self) -> str:
        dir_name = os.path.dirname(self._output_file)
        virtual_env = os.path.join(dir_name, '.python-deps')
        if not os.path.isdir(virtual_env):
            result = self.run_python_command('venv', virtual_env)
            if result.returncode != 0:
                print('Non-zero return code for creating virtualenv!')
                sys.exit(1)
        return virtual_env

    def install(self):
        if not self._python_executable:
            raise ValueError('Could not find system wide executable!')
        if not os.path.isfile(self._python_script):
            raise ValueError(f'Could not find python script'
                             f' {self._python_script}')

        if self._dependencies:
            # Make sure there is a virtual environment
            virtual_env = self.get_virtual_env_dir()
            executable = os.path.join(virtual_env, 'bin', 'python')
            # Install the dependencies (and also upgrade pip)
            self.run_python_command('pip', 'install', '--upgrade', 'pip',
                                    python_exec=executable)
            for dependency in self._dependencies:
                self.run_python_command('pip', 'install', dependency,
                                        python_exec=executable)
            activate = os.path.join(virtual_env, 'bin', 'activate')
            self._commands.append(f'source {activate}')
        script_dir = os.path.dirname(self._python_script)
        self._commands.append(f'PYTHONPATH=$PYTHONPATH:{script_dir}')
        self._commands.append(f'python {self._python_script} $*\n')
        super().install()


def _r(path):
    dir_name = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(dir_name, path)


AVAILABLE_EXECUTABLES = [
    PythonExecutable(_r('download_urls.py'), 'requests'),
    PythonExecutable(_r('right_strip.py'), 'python-magic'),
    PythonExecutable(_r('checksum.py')),
    PythonExecutable(_r('byte_copy.py')),
    PythonExecutable(_r('deduplicate_files/deduplicate.py')),
]

if __name__ == '__main__':
    import argparse
    import platform

    system = platform.system()

    if system != 'Linux' and system != 'Darwin':
        print('Only Linux and Darwin platforms are currently supported')
        sys.exit(1)

    if sys.version_info < (3, 6):
        print('Python version >= 3.6 required to run most scripts!')
        sys.exit(1)

    choice_mapping = {executable.get_name(): executable
                      for executable in AVAILABLE_EXECUTABLES}
    argument_parser = argparse.ArgumentParser(
        description='Installs an executable to a folder which has to be on a'
                    ' path. In case of a python script, a file is generated '
                    'which bootstraps the python environment and executes the'
                    ' file in this directory (thus don\'t delete this '
                    'directory afterwards!)')
    argument_parser.add_argument(
        '--install_directory', default='~/bin',
        help='the directory to install the executable in, which is supposed to'
             ' be available on your path')
    argument_parser.add_argument(
        '--install', nargs='+', choices=choice_mapping.keys(),
        help='Only install the defined executables')
    parsed = argument_parser.parse_args()

    to_install = parsed.install
    if not to_install:
        to_install = choice_mapping.keys()
    install_dir = os.path.abspath(
        os.path.expanduser(parsed.install_directory))
    if not os.path.isdir(install_dir):
        print(f'{install_dir} is not a directory!')
        sys.exit(1)

    for executable_name in to_install:
        executable = choice_mapping[executable_name]
        executable.attach_output_dir(install_dir)
        try:
            executable.install()
        except ValueError as e:
            print(f'Could not install {executable.get_name()}')
            print('\tError:')
            print('\n'.join([
                    f'\t{line}'
                    for line in str(e).split('\n')]))
