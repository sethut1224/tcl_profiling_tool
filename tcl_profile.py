
import os
import sys
import math

from rclpy.serialization import deserialize_message
from rosidl_runtime_py.utilities import get_message
from common import get_rosbag_options
from tcl_std_msgs.msg import ProfileData

import rosbag2_py

import numpy as np
import argparse


def top_of_cum_dist(cum_list, prob_th):
    """
    return index of 'cumulative_prob_list' that satisfies prob_list[index] >= '1-prob_th'
    """
    if prob_th < .0 or prob_th > 1.0:
        return None
    
    max_prob = np.max(cum_list)
    prob_th = min(1 - prob_th, max_prob)

    for idx in range(0, len(cum_list)):
        if cum_list[idx] >= prob_th:
            return idx
    return None

def make_hist(result_dict, hist_type):
    hist_dict = dict()

    for result in result_dict:
        name = result
        data = [int(round(x)) for x in result_dict[name]]
        hist = np.zeros(1000, dtype=int)
        for i in range(0, len(data)):
            hist[data[i]] = hist[data[i]]+1
        
        if hist_type == 'prob':
            denom = float(sum(hist))
            hist = [x / denom for x in hist]    
        hist = list(hist)
        hist_dict[name] = hist
    
    return hist_dict

def make_txt_result(result_dict, result_dir, result_type):

    f = None

    if result_type == 'et_prob':
        f = open(result_dir+'et_prob.txt','w')

    elif result_type == 'et_hist':
        f = open(result_dir+'et_hist.txt','w')

    elif result_type == 'e2e_prob':
        f = open(result_dir+'e2e_prob.txt','w')

    elif result_type == 'e2e_hist':
        f = open(result_dir+'e2e_hist.txt', 'w')

    elif result_type == 'rt_hist':
        f = open(result_dir+'rt_hist.txt','w')

    elif result_type == 'rt_prob':
        f = open(result_dir+'rt_prob.txt','w')

    elif result_type == 'et_orig':
        f = open(result_dir+'et_orig_list.txt','w')

    for result in result_dict:
        name = result
        data = result_dict[result]

        f.write(name+'='+str(data))
        f.write('\n')


def raw_profile(dir_name, cpu_infos):

    task_result = dict()
    cpu_result = dict()

    bag_name= os.listdir(dir_name)[0]

    storage_options, converter_options = get_rosbag_options(dir_name + bag_name)

    reader = rosbag2_py.SequentialReader()
    reader.open(storage_options, converter_options)

    topic_types = reader.get_all_topics_and_types()
    type_map = {topic_types[i].name: topic_types[i].type for i in range(len(topic_types))}    

    while reader.has_next():
        (topic, data, t) = reader.read_next()
        msg_type = get_message(type_map[topic])
        msg = deserialize_message(data, msg_type)
        if task_result.get(topic) == None:
            task_result[topic] = list()
        task_result[topic].append(msg) #태스크 단위로 profile_data 저장
                                       #################################################
                                       # task_result dictionary 구조
                                       # {
                                       #   task1/tcl_profile_data : [msg1, msg2 ...]
                                       #   task2/tcl_profile_data : [msg1, msg2 ...]
                                       #  ...
                                       # }
                                       ##################################################

    for cpu, tasks in enumerate(cpu_infos):
        for task in tasks:
            timing_topic =  '/'+ task + '/tcl_profile_data'
            if task_result.get(timing_topic) != None:
                if cpu_result.get(cpu) == None:
                    cpu_result[cpu] = list()
                for msg in task_result[timing_topic]:
                    cpu_result[cpu].append([msg.timing_header, msg.execution_time.start, msg.execution_time.end, msg.response_time.start, msg.response_time.end])
        cpu_result[cpu].sort(key=lambda x:x[1])
                                                #task 가 구동되는 cpu 단위로 실행 시작 순서에 따라 profile_data 저장
                                                ###########################################################################
                                                # cpu_result 구조
                                                # {
                                                #   cpu1 : [cpu1_task1, cpu1_task2, cpu1_task1, cpu1_task2 ....]
                                                #   cpu2 : [cpu2_task1, cpu2_task2, cpu2_task1, cpu2_task2 ....]
                                                #   ...
                                                # }
                                                # 실행시간 분포, 지연시간 분포 생성 시 태스크들의 실행 시작 순서가 필요하기 때문에 정렬 필요
                                                ###########################################################################

    return cpu_result




def process_execution_time_orig(profile_data): #실행시간을 한 태스크의 시작 - 종료로 정의한 오리지널 버전
    
    execution_time_result = dict()

    for cpu, profile in profile_data.items():
        for data in profile:
            task_name = data[0].task_name
            elapse = (data[2] - data[1]) * 1.0e-6 #msg.execution_time.end - msg.execution_time.start
            elapse_ceil = math.ceil(elapse)

            if execution_time_result.get(task_name) == None:
                execution_time_result[task_name] = list()
            
            execution_time_result[task_name].append(elapse_ceil) #task 단위로 execution time 의 history 저장
                                                                 ########################################
                                                                 # execution_time_result 구조
                                                                 # {
                                                                 #  task1 : [1, 5, 3, 4, 3, 2 ...]
                                                                 #  task2 : [12, 15, 13, 12, 14, 11 ...]
                                                                 #  ...
                                                                 # }
    
    for key, value in execution_time_result.items():
        print(key + ' execution time')
        print('max' , round(max(value)))
        print('avg' , round(sum(value)/len(value)))
        print('min' , round(min(value)))
        print('------')
    
    return execution_time_result

def process_execution_time_task_to_task(task_cpu_infos, profile_data): #실행시간을 선행 태스크 시작 - 후행 태스크 시작으로 정의한 버전
    
    execution_time_result = dict()

    for cpu, profile in profile_data.items():
        sink_task_in_cpu = task_cpu_infos[int(cpu)][-1]
        for i in range(0, len(profile)):
            timing_info = profile[i][0]
            task_name = timing_info.task_name
            
            elapse = (profile[i][2] - profile[i][1]) * 1.0e-6
            elapse_ceil = math.ceil(elapse)

            if task_name != sink_task_in_cpu and i+1 < len(profile):
                    elapse = (profile[i+1][1] - profile[i][1]) * 1.0e-6
                    elapse_ceil = math.ceil(elapse)
                
            if execution_time_result.get(task_name) == None:
                execution_time_result[task_name] = list()
        
            execution_time_result[task_name].append(elapse_ceil) #task 단위로 execution time 의 history 저장
                                                                 ########################################
                                                                 # execution_time_result 구조
                                                                 # {
                                                                 #  task1 : [1, 5, 3, 4, 3, 2 ...]
                                                                 #  task2 : [12, 15, 13, 12, 14, 11 ...]
                                                                 #  ...
                                                                 # }
    
    for key, value in execution_time_result.items():
        print(key + ' execution time')
        print('max' , round(max(value)))
        print('avg' , round(sum(value)/len(value)))
        print('min' , round(min(value)))
        print('------')
    
    return execution_time_result

def process_response_time(profile_data):

    response_time_result = dict()

    for cpu, profile in profile_data.items():
        for data in profile:
            task_name = data[0].task_name
            elapse = (data[4] - data[3]) * 1.0e-6 # msg.response_time.end - msg.response_time.start

            if response_time_result.get(task_name) == None:
                response_time_result[task_name] = list()

            response_time_result[task_name].append(elapse) #task 단위로 response time 의 history 저장
                                                           ########################################
                                                           # response_time_result 구조
                                                           # {
                                                           #  task1 : [1, 5, 3, 4, 3, 2 ...]
                                                           #  task2 : [12, 15, 13, 12, 14, 11 ...]
                                                           #  ...
                                                           # }
    
    for key, value in response_time_result.items():
        print(key + ' response time')
        print('max' , round(max(value)))
        print('avg' , round(sum(value)/len(value)))
        print('min' , round(min(value)))
        print('------')

    return response_time_result

def process_e2e_latency_task_to_task(profile_data, paths):

    e2e_latency_result = dict()

    for path in paths:
        source_task_name = path[0]
        sink_task_name = path[-1]
        source_task_cpu = None
        sink_task_cpu = None

        objective_path_str = str(path) #관심 경로

        sink_result = dict()

        for cpu, profile in profile_data.items():
            for data in profile:
                if data[0].task_name == source_task_name:
                    source_task_cpu = cpu
                if data[0].task_name == sink_task_name:
                    sink_task_cpu = cpu
                if source_task_cpu is not None and sink_task_cpu is not None:
                    break 
        #profile data 가 cpu 단위로 저장되기 때문에 source, sink task 의 cpu 번호 저장        

        for data in profile_data[sink_task_cpu]:
            if data[0].task_name == sink_task_name:
                for msg_info in data[0].msg_infos:
                    task_history_set = set(msg_info.task_history)
                    path_set = set(path)
                    if path_set.intersection(task_history_set) == path_set: #timing info 에 objective_path 가 존재하는지
                        sink_result[msg_info.msg_id] = data[4]   
                        break                                   #msg_id 단위로 sink task 의 response_time.end 저장
                                                                ########################################
                                                                # sink_result 구조
                                                                # {
                                                                #  1 : [165423434.12351231]
                                                                #  2 : [165423434.15431230]
                                                                #  ...
                                                                # }   

        for data in profile_data[source_task_cpu]:
            if data[0].task_name == source_task_name:
                for msg_info in data[0].msg_infos:
                    if sink_result.get(msg_info.msg_id) is not None: #sink 와 source 의 msg_id 가 같으면
                        elapse = (sink_result[msg_info.msg_id] - data[1]) * 1.0e-6 #sink_task response_time.end - source task response_time.start (or msg create time)
                        if e2e_latency_result.get(objective_path_str) == None:
                            e2e_latency_result[objective_path_str] = list()
                        e2e_latency_result[objective_path_str].append(elapse)
                        break                                                #object_path 단위로 e2e latency history 저장
                                                                             ########################################
                                                                             # e2e_latency_result 구조
                                                                             # {
                                                                             #  obj_path1 : [32.1, 35.23, 30.11 ....]
                                                                             #  obj_path2 : [78.21, 77.09, 80.43 ....]
                                                                             #  ...
                                                                             # }  
                            
    for key, value in e2e_latency_result.items():
        print(key + ' e2e latency')
        print('max' , round(max(value)))
        print('min' , round(min(value)))
        print('avg' , round(sum(value)/len(value)))
        print('------')     

    return e2e_latency_result

def main(args):
    input_dir = args.input
    output_dir = args.output

    # 태스크 - 코어 할당 정보
    cpu0 = ['front_lidar_driver', 'point_cloud_fusion', 'voxel_grid_filter', 'ndt_localizer']
    cpu1 = ['rear_lidar_driver', 'point_cloud_fusion', 'ray_ground_filter', 'euclidean_clustering']

    task_cpu_infos = [cpu0, cpu1]

    # e2e latency 를 얻고자하는 관심 경로 (경로 상의 모든 태스크 입력)
    e2e_paths = [
        ['front_lidar_driver', 'point_cloud_fusion', 'voxel_grid_filter', 'ndt_localizer'],
        ['front_lidar_driver', 'point_cloud_fusion', 'ray_ground_filter', 'euclidean_clustering'],
        ['rear_lidar_driver', 'point_cloud_fusion', 'voxel_grid_filter', 'ndt_localizer'],
        ['rear_lidar_driver', 'point_cloud_fusion', 'ray_ground_filter', 'euclidean_clustering'],
    ]

    profile_data = raw_profile(input_dir, task_cpu_infos) #rosbag 을 읽고, cpu 단위로 task 들의 profile data 저장

    # execution_time_data = process_execution_time_orig(profile_data) #profile_data 를 사용하여 task 들의 execution_time 분포 획득
                                                                    # 실행시간을 한 태스크의 시작 - 종료로 정의한 오리지널 버전

    execution_time_data = process_execution_time_task_to_task(task_cpu_infos, profile_data) #profile_data 를 사용하여 task 들의 execution_time 분포 획득
                                                                            #실행시간을 선행 태스크의 시작 - 후행 태스크의 시작으로 정의한 버전

    e2e_latency_data    = process_e2e_latency_task_to_task(profile_data, e2e_paths) #profile_data 를 사용하여 e2e_path 의 e2e_latency 분포 획득
    
    execution_time_prob = make_hist(execution_time_data, 'prob') #각 task 의 execution time 확률 분포 획득
    e2e_latency_prob = make_hist(e2e_latency_data, 'prob') #각 관심 경로의 e2e latency 확률 분포 획득
    
    make_txt_result(execution_time_prob, output_dir, 'et_prob') #execution time 확률 분포를 txt 파일로 저장    
    make_txt_result(e2e_latency_prob, output_dir, 'e2e_prob') #e2e latency 확률 분포를 txt 파일로 저장

    ###response time###
    # response_time_data  = process_response_time(profile_data) #profile_data 를 사용하여 task 들의 response_time 분포 획득
    # response_time_prob = make_hist(response_time_data, 'prob') #각 task 의 response time 확률 분포 획득
    # make_txt_result(response_time_prob, output_dir, 'rt_prob') #response time 확률 분포를 txt 파일로 저장

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='input profiled directory name', required=True)
    parser.add_argument('-o', '--output', help='output result directory name', required=True)

    args = parser.parse_args()

    main(args)