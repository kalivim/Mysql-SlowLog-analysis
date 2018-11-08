#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/10/12 下午3:00
# @Author  : Kionf
# @Site    : https://kionf.com
# @Software: Sublime

import subprocess
import json
import os
import re
import sys
import time
from jinja2 import Environment, FileSystemLoader

#reload(sys)
#sys.setdefaultencoding('utf-8')

class RunAndCheckCommand:

    def __init__(self, commands, task_name, ret_code=0):
        self.commands = commands
        self.task_name = task_name
        self.ret_code = ret_code

    def check_command_status_code(self):
        """
        检测任务
        """
        if self.exp_code == self.ret_code:
            print("\033[92m [INFO]>> %s  \033[0m" % self.task_name)
        else:
            print("\033[91m [ERROR]>> %s %s  \033[0m" % (self.task_name, self.stderr))
            exit(1)

    def exec_command_stdout_res(self):
        """
        执行命令实时返回命令输出
        :return:
        """
        command_res = subprocess.Popen(self.commands, shell=True)
        self.stderr = command_res.stderr
        while command_res.poll():
            line = command_res.stdout.readline()
            line.strip()
            if line:
                print(line)
        command_res.wait()
        self.exp_code = command_res.returncode
        self.check_command_status_code()


class AnalysisMysqlSlowLog:
    """
    分析Mysql慢查询日志输出报告。
    调用第三方工具包percona-toolkit中pt-query-digest工具，默认输出slow.json文件Perl语言编写
    """

    def __init__(self, slow_log_file, json_file, report_file):
        """
        :param slow_log_file: 需要分析的慢查询日志文件
        :param report_file: 生成报告文件名
        """
        self.LibToolkit = './pt-query-digest'
        self.json_file = json_file
        self.report_file = report_file
        self.slow_log_file = slow_log_file
        self.query_digest = "%s  %s --output json --progress time,1 > %s 2>/dev/null" % (
            self.LibToolkit, slow_log_file, self.json_file)

    def general_html_report(self, sql_info):
        #env = Environment(loader=PackageLoader(os.path.split(os.getcwd())[1]))
        env = Environment(loader=FileSystemLoader(os.path.dirname(__file__)))
        template = env.get_template('template.html')
        html_content = template.render(sql_info=sql_info)
        with open(self.report_file, 'wa') as f:
            f.write(html_content.encode('utf-8'))

    def general_json_slow_log_report(self):
        """
        调用第三方工具pt-query-digest生成json报告,并获取需要信息
        :return: digest slow_log format to json
        """
        RunCommandsOBJ = RunAndCheckCommand(self.query_digest, '生成Json报告')
        RunCommandsOBJ.exec_command_stdout_res()
        f = open(self.json_file, 'ra')
        format_dict_all_data = json.load(f)
        have_slow_query_tables = []
        all_sql_info = []
        all_slow_query_sql_info = format_dict_all_data['classes']
        global_sql_info = format_dict_all_data['global']

        for slow_query_sql in all_slow_query_sql_info:
            query_metrics = slow_query_sql['metrics']
            query_time = query_metrics['Query_time']
            query_tables = slow_query_sql['tables']

            for show_tables_sql in query_tables:
                get_table_name = show_tables_sql['create'].split('.')[1]
                table_name = re.match(r'`(\w*)`\\G', get_table_name).group(1)
                if table_name not in have_slow_query_tables:
                    have_slow_query_tables.append(table_name)

            sql_info = {
                'ID': slow_query_sql['checksum'],
                'query_time_max': query_time['max'],
                'query_time_min': query_time['min'],
                'query_time_95': query_time['pct_95'],
                'query_time_median': query_time['median'],
                'query_row_send_95': query_metrics['Rows_sent']['pct_95'],
                'query_db': query_metrics['db']['value'],
                'slow_query_count': slow_query_sql['query_count'],
                'slow_query_tables': have_slow_query_tables,
                'sql': slow_query_sql['example']['query'],

            }

            all_sql_info.append(sql_info)
            all_sql_info = sorted(all_sql_info, key=lambda e: float(e['query_time_95']), reverse=True)
            #print(all_sql_info.__getitem__(0))
        self.general_html_report(all_sql_info)


if __name__ == "__main__":
    if len(sys.argv) == 4:
        slow_log_name = sys.argv[1]
        json_file_name = sys.argv[2]
        report_name = sys.argv[3]
        obj = AnalysisMysqlSlowLog(slow_log_file=slow_log_name, json_file=json_file_name, report_file=report_name)
        obj.general_json_slow_log_report()
    else:
       pass

