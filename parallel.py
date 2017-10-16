import sys
import os
import datetime
from multiprocessing import Pool, cpu_count
from subprocess import Popen, PIPE
from itertools import product

TAGS = {'id=test1', 'id=test2', 'id=test3', 'id=test4', 'id=test5'}


class LoggerToFile:

    def __init__(self):
        pass

    def prepare_logs_folder(self):
        current_date_and_time = datetime.datetime.now()
        logs_path = "log/{0}".format(current_date_and_time.strftime("%Y-%m-%dT%H_%M"))
        if not os.path.exists(logs_path):
            os.makedirs(logs_path)
        return logs_path

    def log_output(self, stdout, log_directory, tag_arguments, pid):
        # TODO: split logging to console and file
        log_file = open('{0}/{1}.txt'.format(log_directory, tag_arguments), 'w')
        for line in stdout:
            sys.stdout.write("PID: {0} - {1}".format(pid, line))
            log_file.write(line)


class Tag:

    def __init__(self):
        pass

    def prepare_dict_with_tags_chunks(self, tags_list, chunks_number):
        tags_lists_dict = dict()
        for i in xrange(0, len(tags_list)):
            if i % chunks_number not in tags_lists_dict.keys():
                tags_lists_dict[i % chunks_number] = list()
            tags_lists_dict[i % chunks_number].append(tags_list[i])
        return tags_lists_dict


class Feature:

    def __init__(self):
        pass

    def run_features_unpack(self, args):
        return self.run_features(*args)

    def run_features(self, tags_list, log_directory):
        tag_parameters = self.prepare_tags_parameters(tags_list)
        command = 'lettuce features {0}'.format(tag_parameters)
        process = ParallelProcess().run_process(command.split())
        LoggerToFile().log_output(process.stdout, log_directory, tag_parameters, process.pid)
        return process.wait()

    def prepare_tags_parameters(self, tags_list):
        tag_template = '--tag={0}'
        tag_parameters = tag_template.format(tags_list[0])
        tags_list_length = len(tags_list)
        if tags_list_length > 1:
            for i in range(1, tags_list_length):
                tag_parameters += ' {0}'.format(tag_template.format(tags_list[i]))
        return tag_parameters

# TODO: remove after tests
def run_features_unpack_2(args):
   return Feature().run_features(*args)

class ParallelProcess:

    def __init__(self):
        pass

    def open_parallel_processes(self, processes_count, function, iterable):
        pool = Pool(processes=processes_count)
        # TODO: fix function call
        # https://stackoverflow.com/questions/3288595/multiprocessing-how-to-use-pool-map-on-a-function-defined-in-a-class
        pool.map(function, iterable)

    def run_process(self, process_command):
        process = Popen(process_command, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        return process


class Iterable:

    def __init__(self):
        pass

    def make_iterable_from_string(self, string):
        return {0: string}.values()

    def make_product_iterable(self, *args):
        return product(*args)


if __name__ == '__main__':
    processes_count = cpu_count()
    tags_lists_dict = Tag().prepare_dict_with_tags_chunks(list(TAGS), processes_count)
    logs_path = LoggerToFile().prepare_logs_folder()
    logs_path_iterable = Iterable().make_iterable_from_string(logs_path)
    iterable_function_arguments = Iterable().make_product_iterable(tags_lists_dict.values(), logs_path_iterable)

    ParallelProcess().open_parallel_processes(processes_count, Feature().run_features_unpack.im_func, iterable_function_arguments)
