import sys
import os
from datetime import datetime
from logging import Logger
from multiprocessing import Pool, cpu_count
from subprocess import Popen, PIPE, STDOUT
from itertools import product


# Out of class due to Pool.map limitations on calling methods inside a class
def unpack_run_features_function(args):
    return Feature().run_features(*args)


class ParallelLogger(Logger):

    def __init__(self):
        self._log_template = "PID: {0} - {1} - {2}"
        self._logs_path_template = "ParallelRunnerLogs/{0}"
        self._log_file_template = "{0}/{1}.txt"
        self._time_log_format = "%H:%M:%S"
        self._logs_folder_date_time_name_format = "%Y-%m-%d_T_%H-%M"
        super(ParallelLogger, self).__init__("ParallelLogger")

    def prepare_logs_folder(self):
        current_date_and_time = datetime.now()
        logs_folder_path = self._logs_path_template.\
            format(current_date_and_time.strftime(self._logs_folder_date_time_name_format))
        if not os.path.exists(logs_folder_path):
            os.makedirs(logs_folder_path)
        return logs_folder_path

    def log_on_console(self, log_text, pid):
        log_time_raw = datetime.now()
        log_time = log_time_raw.strftime(self._time_log_format)
        sys.stdout.write(self._log_template.format(pid, log_time, log_text))

    def log_to_file(self, line, log_directory, tag_parameters, pid):
        log_time_raw = datetime.now()
        log_time = log_time_raw.strftime(self._time_log_format)
        with open(self._log_file_template.format(log_directory, tag_parameters), "ab") \
                as log_file:
            log_file.write(self._log_template.format(pid, log_time, line))


class Tag(object):

    def __init__(self):
        self._tags_lists_dict = dict()

    def prepare_dict_with_tags_chunks(self, tags_list, chunks_number):
        for i in xrange(0, len(tags_list)):
            if i % chunks_number not in self._tags_lists_dict.keys():
                self._tags_lists_dict[i % chunks_number] = list()
            self._tags_lists_dict[i % chunks_number].append(tags_list[i])
        return self._tags_lists_dict


class Feature(object):

    def __init__(self):
        self._lettuce_command_template = 'lettuce features {0} --with-xunit'
        self._tag_template = '--tag={0}'
        self._parallel_process = ParallelProcess()
        self._logger = ParallelLogger()

    def run_features(self, tags_list, log_directory):
        tag_parameters = self.prepare_tags_parameters(tags_list)
        command_to_run = self._lettuce_command_template.format(tag_parameters)
        command = self._parallel_process.run_command(command_to_run.split(), log_directory, tag_parameters)
        command_results_unpack = command.communicate()
        self._logger.log_on_console(command_results_unpack[1], command.pid)
        self._logger.log_to_file(command_results_unpack[1], log_directory, tag_parameters, command.pid)
        self._parallel_process.close_process(command)

    def prepare_tags_parameters(self, tags_list):
        tag_parameters = self._tag_template.format(tags_list[0])
        tags_list_length = len(tags_list)
        if tags_list_length > 1:
            for i in range(1, tags_list_length):
                tag_parameters += ' {0}'.format(self._tag_template.format(tags_list[i]))
        return tag_parameters


class ParallelProcess(object):

    def __init__(self):
        self._logger = ParallelLogger()

    @staticmethod
    def open_parallel_processes(processes_number, function_to_map, iterable):
        pool = Pool(processes=processes_number)
        pool.map(function_to_map, iterable)

    @staticmethod
    def close_process(process):
        process.kill()

    def run_command(self, command, log_directory, tag_parameters):
        command_results = Popen(command, stdin=PIPE, stdout=PIPE, stderr=STDOUT)
        for stdout_line in iter(command_results.stdout.readline, ""):
            self._logger.log_on_console(stdout_line, command_results.pid)
            self._logger.log_to_file(stdout_line, log_directory, tag_parameters,
                                     command_results.pid)
        return command_results


class Iterable(object):

    def __init__(self):
        pass

    @staticmethod
    def make_iterable_from_string(string):
        return [string]

    @staticmethod
    def make_product_iterable(*args):
        return product(*args)


if __name__ == '__main__':
    EXAMPLE_TAGS = {'id=test1', 'id=test2', "id=test5", "id=test4"}
    processes_count = cpu_count()
    tags_lists_dict = Tag().prepare_dict_with_tags_chunks(list(EXAMPLE_TAGS), processes_count)
    logs_path = ParallelLogger().prepare_logs_folder()
    logs_path_iterable = Iterable().make_iterable_from_string(logs_path)
    iterable_function_arguments = Iterable().make_product_iterable(tags_lists_dict.values(), logs_path_iterable)

    ParallelProcess().open_parallel_processes(processes_count, unpack_run_features_function, iterable_function_arguments)