#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import urllib2
import wget
import csv


class MawiNetworkTrafficSpider(object):
    """
    spider for http://mawi.wide.ad.jp/~agurim/dataset/
    """
    def __init__(self,
                 url_root,
                 dump_root_dir,
                 dump_root_csv_dir):
        """
        initialization for spider

        :param url_root: url root from which to dump all the raw data
        :param dump_root_dir:
        :param dump_root_csv_dir:
        """
        self._url_root = url_root
        self._dump_root_dir = dump_root_dir
        self._dump_root_csv_dir = dump_root_csv_dir

        if not os.path.exists(self._dump_root_dir):
            os.makedirs(self._dump_root_dir)

        if not os.path.exists(self._dump_root_csv_dir):
            os.makedirs(self._dump_root_csv_dir)

        return

    def run(self):
        """
        dump all files from the configured url and all its sub-urls
        local file(s)

        :return:
        """
        file_or_folders = self.parser_name(self._url_root)
        for appendix in file_or_folders:
            month_url = self._url_root + appendix

            # .agr file
            if month_url.endswith('.agr'):
                print '\nDownloading {}...'.format(month_url)
                if not os.path.exists(os.path.join(self._dump_root_dir, appendix)):
                    wget.download(month_url, out=self._dump_root_dir)

            # folder
            else:
                year_month_day_list = self.parser_name(month_url)
                for ymd in year_month_day_list:
                    day_url = month_url + ymd
                    if day_url.endswith('.agr'):
                        print '\nDownloading {}...'.format(day_url)
                        if not os.path.exists(os.path.join(self._dump_root_dir, ymd)):
                            wget.download(day_url, out=self._dump_root_dir)
                    else:
                        agr_name = '{}.agr'.format(ymd.split('/')[0])
                        day_url_full = day_url + agr_name
                        print '\nDownloading {}...'.format(day_url_full)
                        if not os.path.exists(os.path.join(self._dump_root_dir, agr_name)):
                            wget.download(day_url_full, out=self._dump_root_dir)
        return

    @staticmethod
    def parser_name(root_url):
        """
        parser file or folder in the root url

        :return:
        """
        html = urllib2.urlopen(root_url).read()
        items = html.split('</a></li>')
        ymd_names = list()
        for item in items:
            name = item.split('<li><a')[-1].split('> ')[-1]
            if '/ul' not in name and 'Parent' not in name:
                ymd_names.append(name)

        return ymd_names

    def extract_csv(self):
        """
        extract csv for all dumped *.agr files
        :return:
        """
        agr_names = os.listdir(self._dump_root_dir)
        for i, agr in enumerate(agr_names):
            csv_file_full_path = os.path.join(self._dump_root_csv_dir, agr.split('.')[0] + '.csv')
            wf = open(csv_file_full_path, 'wb')
            wf.write('start_year,start_month,start_day,start_hour,start_minute,start_second,')
            wf.write('end_year,end_month,end_day,end_hour,end_minute,end_second,mbps,pps\n')

            print 'Procssing raw file {}...'.format(os.path.join(self._dump_root_dir, agr))
            with open(os.path.join(self._dump_root_dir, agr)) as f:
                lines = f.readlines()
                for line in lines:
                    if 'StartTime' in line:
                        start_ymd_hms = line.split('(')[1].split(')')[0]
                        start_ymd = start_ymd_hms.split(' ')[0].strip()
                        start_hms = start_ymd_hms.split(' ')[1].strip()
                        start_year, start_month, start_day = start_ymd.split('/')[:]
                        start_hour, start_minute, start_sec = start_hms.split(':')[:]
                        start_year, start_month, start_day, start_hour, start_minute, start_sec = int(start_year), \
                                                                                                  int(start_month), \
                                                                                                  int(start_day), \
                                                                                                  int(start_hour), \
                                                                                                  int(start_minute), \
                                                                                                  int(start_sec)
                        # print start_year, start_month, start_day, start_hour, start_minute, start_sec
                        wf.write('{},{},{},{},{},{},'.format(start_year, start_month, start_day,
                                                             start_hour, start_minute, start_sec))

                    if 'EndTime' in line:
                        end_ymd_hms = line.split('(')[1].split(')')[0]
                        end_ymd = end_ymd_hms.split(' ')[0].strip()
                        end_hms = end_ymd_hms.split(' ')[1].strip()
                        end_year, end_month, end_day = end_ymd.split('/')[:]
                        end_hour, end_minute, end_sec = end_hms.split(':')[:]
                        end_year, end_month, end_day, end_hour, end_minute, end_sec = int(start_year), \
                                                                                      int(end_month), \
                                                                                      int(end_day), \
                                                                                      int(end_hour), \
                                                                                      int(end_minute), \
                                                                                      int(end_sec)
                        # print end_year, end_month, end_day, end_hour, end_minute, end_sec
                        wf.write('{},{},{},{},{},{},'.format(end_year, end_month, end_day,
                                                             end_hour, end_minute, end_sec))

                    if 'AvgRate' in line:
                        if 'Mbps' in line:
                            mbps = float(line.split('AvgRate:')[1].split('Mbps')[0].strip())
                            pps = float(line.split('Mbps')[1].split('pps')[0].strip())
                        elif 'Gbps' in line:
                            mbps = float(line.split('AvgRate:')[1].split('Gbps')[0].strip()) * 1024.0
                            pps = float(line.split('Gbps')[1].split('pps')[0].strip())
                        # print mbps, pps
                        wf.write('{},{}\n'.format(mbps, pps))

            wf.close()
        return


if __name__ == '__main__':
    spider = MawiNetworkTrafficSpider(
        url_root='http://mawi.wide.ad.jp/~agurim/dataset/',
        dump_root_dir='../dataset/',
        dump_root_csv_dir='../csv/'
    )

    # spider.run()
    spider.extract_csv()





