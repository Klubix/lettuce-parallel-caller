import sys
import os
import datetime
from logging import Logger
from multiprocessing import Pool, cpu_count
from subprocess import Popen, PIPE
from itertools import product


TAGS = {'id=test1', 'id=test2', 'id=test3', 'id=test4', 'id=test5'}


# Out of class due to Pool.map limitations on calling methods inside a class
def unpack_run_features_function(args):
    return Feature().run_features(*args)


class CustomLogger(Logger):

    def __init__(self):
        self._log_template = "PID: {0} - {1}"
        self._logs_path = "ParallelRunnerLogs/{0}"
        self._log_file_template = "{0}/{1}.txt"
        super(CustomLogger, self).__init__("CustomLogger")

    def prepare_logs_folder(self):
        current_date_and_time = datetime.datetime.now()
        logs_path = self._logs_path.format(current_date_and_time.strftime("%Y-%m-%dT%H_%M"))
        if not os.path.exists(logs_path):
            os.makedirs(logs_path)
        return logs_path

    def log_on_console(self, line, pid):
        sys.stdout.write(self._log_template.format(pid, line))

    def log_to_file(self, line, log_directory, tag_arguments, pid):
        with open(self._log_file_template.format(log_directory, tag_arguments), "ab") \
                as log_file:
            log_file.write(self._log_template.format(pid, line))


class Tag(object):

    def __init__(self):
        pass

    def prepare_dict_with_tags_chunks(self, tags_list, chunks_number):
        tags_lists_dict = dict()
        for i in xrange(0, len(tags_list)):
            if i % chunks_number not in tags_lists_dict.keys():
                tags_lists_dict[i % chunks_number] = list()
            tags_lists_dict[i % chunks_number].append(tags_list[i])
        return tags_lists_dict


class Feature(object):

    def __init__(self):
        self._lettuce_command_template = 'lettuce features {0} --with-xunit'
        self._tag_template = '--tag={0}'

    def run_features(self, tags_list, log_directory):
        tag_parameters = self.prepare_tags_parameters(tags_list)
        command = self._lettuce_command_template.format(tag_parameters)
        command_results = ParallelProcess().run_command(command.split(), log_directory, tag_parameters)
        process_result = command_results.wait()
        return process_result

    def prepare_tags_parameters(self, tags_list):
        tag_parameters = self._tag_template.format(tags_list[0])
        tags_list_length = len(tags_list)
        if tags_list_length > 1:
            for i in range(1, tags_list_length):
                tag_parameters += ' {0}'.format(self._tag_template.format(tags_list[i]))
        return tag_parameters


class ParallelProcess(object):

    def __init__(self):
        pass

    def open_parallel_processes(self, processes_count, function, iterable):
        pool = Pool(processes=processes_count)
        pool.map(function, iterable)

    def run_command(self, command, log_directory, tag_parameters):
        logger = CustomLogger()
        command_results = Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        for stdout_line in iter(command_results.stdout.readline, ""):
            logger.log_on_console(stdout_line, command_results.pid)
            logger.log_to_file(stdout_line, log_directory, tag_parameters,
                               command_results.pid)
        return command_results


class Iterable(object):

    def __init__(self):
        pass

    def make_iterable_from_string(self, string):
        return [string]

    def make_product_iterable(self, *args):
        return product(*args)


if __name__ == '__main__':
    start_date_and_time = datetime.datetime.now()

    print "STARTED AT: "
    print start_date_and_time.strftime("%Y-%m-%dT%H_%M")

    processes_count = cpu_count()
    tags_lists_dict = Tag().prepare_dict_with_tags_chunks(list(TAGS), processes_count)
    logs_path = CustomLogger().prepare_logs_folder()
    logs_path_iterable = Iterable().make_iterable_from_string(logs_path)
    iterable_function_arguments = Iterable().make_product_iterable(tags_lists_dict.values(), logs_path_iterable)

    ParallelProcess().open_parallel_processes(processes_count, unpack_run_features_function, iterable_function_arguments)

    end_date_and_time = datetime.datetime.now()
    print "STARTED AT: "
    print start_date_and_time.strftime("%Y-%m-%dT%H_%M")
    print "ENDED AT: "
    print end_date_and_time.strftime("%Y-%m-%dT%H_%M")
