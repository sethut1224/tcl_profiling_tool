
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

def get_shortcut_from_task_name(task_name):
    name = ''
    if task_name == 'virtual_driver_vlp16_front':
        name = 'FRONT_LID'
    elif task_name == 'virtual_driver_vlp16_rear':
        name = 'REAR_LID'
    elif task_name == 'filter_transform_vlp16_front':
        name = 'FRONT_PFT'
    elif task_name == 'filter_transform_vlp16_rear':
        name = 'REAR_PFT'
    elif task_name == 'point_cloud_fusion':
        name = 'PCF'
    elif task_name == 'ray_ground_classifier':
        name = 'RGF'
    elif task_name =='euclidean_clustering':
        name = 'EUC'
    elif task_name == 'voxel_grid':
        name = 'VGF'
    elif task_name == 'ndt_localizer':
        name = 'NDT'
    elif task_name == 'virtual_driver_camera':
        name = 'CAM'
    elif task_name == 'tensorrt_yolo':
        name = 'TRT'
    elif task_name == 'vision_detections':
        name = 'VID'
    elif task_name == 'multi_object_tracker':
        name = 'MOT'    
    elif task_name == 'virtual_driver_vehicle_kinematic_state':
        name = 'ODM'
    elif task_name == 'universe_ekf_localizer':
        name = 'EKF'
    elif task_name == 'behavior_planner':
        name = 'BHP'
    elif task_name == 'pure_pursuit':
        name = 'PPS'
    elif task_name == 'simulation/dummy_perception_publisher':
        name = 'DPP'
    elif task_name == 'simulation/detected_object_feature_remover':
        name = 'OFR'
    elif task_name == 'perception/object_recognition/tracking/multi_object_tracker':
        name = 'MOT'
    elif task_name == 'perception/object_recognition/prediction/map_based_prediciton':
        name = 'MBP'
    elif task_name == 'planning/scenario_planning/lane_driving/behavior_planning/behavior_path_planner':
        name = 'BPP'
    elif task_name == 'planning/scenario_planning/lane_driving/behavior_planning/behavior_velocity_planner':
        name = 'BVP'
    elif task_name == 'planning/scenario_planning/lane_driving/motion_planning/obstacle_avoidance_planner':
        name = 'OAP'
    elif task_name == 'planning/scenario_planning/lane_driving/motion_planning/obstacle_cruise_planner':
        name = 'OCP'
    elif task_name == 'planning/scenario_planning/motion_velocity_smoother':
        name = 'MVS'
    elif task_name == 'control/trajectory_follower/mpc_follower':
        name = 'MPC'
    else:
        name = ''
    return name

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
        task_result[topic].append(msg) #????????? ????????? profile_data ??????
                                       #################################################
                                       # task_result dictionary ??????
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
                                                #task ??? ???????????? cpu ????????? ?????? ?????? ????????? ?????? profile_data ??????
                                                ###########################################################################
                                                # cpu_result ??????
                                                # {
                                                #   cpu1 : [cpu1_task1, cpu1_task2, cpu1_task1, cpu1_task2 ....]
                                                #   cpu2 : [cpu2_task1, cpu2_task2, cpu2_task1, cpu2_task2 ....]
                                                #   ...
                                                # }
                                                # ???????????? ??????, ???????????? ?????? ?????? ??? ??????????????? ?????? ?????? ????????? ???????????? ????????? ?????? ??????
                                                ###########################################################################

    return cpu_result




def process_execution_time_orig(profile_data): #??????????????? ??? ???????????? ?????? - ????????? ????????? ???????????? ??????
    
    execution_time_result = dict()

    for cpu, profile in profile_data.items():
        for data in profile:
            task_name = data[0].task_name
            elapse = (data[2] - data[1]) * 1.0e-6 #msg.execution_time.end - msg.execution_time.start
            elapse_ceil = math.ceil(elapse)

            if execution_time_result.get(task_name) == None:
                execution_time_result[task_name] = list()
            
            print(task_name, elapse_ceil)
            execution_time_result[task_name].append(elapse_ceil) #task ????????? execution time ??? history ??????
                                                                 ########################################
                                                                 # execution_time_result ??????
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

def process_execution_time_task_to_task(task_cpu_infos, profile_data): #??????????????? ?????? ????????? ?????? - ?????? ????????? ???????????? ????????? ??????
    
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
        
            execution_time_result[task_name].append(elapse_ceil) #task ????????? execution time ??? history ??????
                                                                 ########################################
                                                                 # execution_time_result ??????
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

            response_time_result[task_name].append(elapse) #task ????????? response time ??? history ??????
                                                           ########################################
                                                           # response_time_result ??????
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

def process_e2e_latency_using_source_sink(profile_data, paths):

    e2e_latency_result = dict()

    for path in paths:
        source_task_name = path[0]
        sink_task_name = path[-1]
        source_task_cpu = None
        sink_task_cpu = None

        objective_path_str = str(path) #?????? ??????

        sink_result = dict()

        for cpu, profile in profile_data.items():
            for data in profile:
                if data[0].task_name == source_task_name:
                    source_task_cpu = cpu
                if data[0].task_name == sink_task_name:
                    sink_task_cpu = cpu
                if source_task_cpu is not None and sink_task_cpu is not None:
                    break 
        #profile data ??? cpu ????????? ???????????? ????????? source, sink task ??? cpu ?????? ??????        

        for data in profile_data[sink_task_cpu]:
            if data[0].task_name == sink_task_name:
                for msg_info in data[0].msg_infos:
                    task_history_set = set(msg_info.task_history)
                    path_set = set(path)
                    if path_set.intersection(task_history_set) == path_set: #timing info ??? objective_path ??? ???????????????
                        sink_result[msg_info.msg_id] = data[4]   
                        break                                   #msg_id ????????? sink task ??? response_time.end ??????
                                                                ########################################
                                                                # sink_result ??????
                                                                # {
                                                                #  1 : [165423434.12351231]
                                                                #  2 : [165423434.15431230]
                                                                #  ...
                                                                # }   

        for data in profile_data[source_task_cpu]:
            if data[0].task_name == source_task_name:
                for msg_info in data[0].msg_infos:
                    if sink_result.get(msg_info.msg_id) is not None: #sink ??? source ??? msg_id ??? ?????????
                        elapse = (sink_result[msg_info.msg_id] - data[0].msg_infos.creation_time) * 1.0e-6 #sink_task response_time.end - source task response_time.start (or msg create time)
                        if e2e_latency_result.get(objective_path_str) == None:
                            e2e_latency_result[objective_path_str] = list()
                        e2e_latency_result[objective_path_str].append(elapse)
                        break                                                #object_path ????????? e2e latency history ??????
                                                                             ########################################
                                                                             # e2e_latency_result ??????
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

def process_e2e_latency_using_sink(profile_data, paths):

    e2e_latency_result = dict()

    for key, path in paths.items():
        sink_task_name = path[-1]
        sink_task_cpu = None

        objective_path_str = str(path) #?????? ??????

        sink_result = dict()

        msg_ids = []
        for cpu, profile in profile_data.items():
            for data in profile:
                if data[0].task_name == sink_task_name:
                    sink_task_cpu = cpu
                if sink_task_cpu is not None:
                    break 
        #profile data ??? cpu ????????? ???????????? ????????? source, sink task ??? cpu ?????? ??????        

        for data in profile_data[sink_task_cpu]:
            if data[0].task_name == sink_task_name:
                for msg_info in data[0].msg_infos:
                    task_history_set = set(msg_info.task_history)
                    path_set = set(path)
                    if path_set.intersection(task_history_set) == path_set: #timing info ??? objective_path ??? ???????????????
                        elapse = (data[2] - msg_info.creation_time) * 1.0e-6
                        if e2e_latency_result.get(key) == None:
                            e2e_latency_result[key] = list()
                        if msg_info.msg_id not in msg_ids:
                            e2e_latency_result[key].append(elapse)   
                            msg_ids.append(msg_info.msg_id)
                        break                                   #msg_id ????????? sink task ??? response_time.end ??????
                                                                ########################################
                                                                # sink_result ??????
                                                                # {
                                                                #  1 : [165423434.12351231]
                                                                #  2 : [165423434.15431230]
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

    # ????????? - ?????? ?????? ??????
    cpu0 = ['virtual_driver_vlp16_front',  'point_cloud_fusion', 'voxel_grid', 'ndt_localizer']
    cpu1 = ['virtual_driver_vlp16_rear',   'point_cloud_fusion', 'ray_ground_classifier', 'euclidean_clustering']
    cpu2 = ['virtual_driver_camera', 'tensorrt_yolo', 'vision_detections', 'multi_object_tracker']
    cpu3 = ['virtual_driver_vehicle_kinematic_state', 'universe_ekf_localizer', 'behavior_planner', 'pure_pursuit']
    
    task_cpu_infos = [cpu0, cpu1, cpu2, cpu3]

    # e2e latency ??? ??????????????? ?????? ?????? (?????? ?????? ?????? ????????? ??????)
    e2e_paths = {
        'FRONT_LIDAR_LOC' : ['virtual_driver_vlp16_front',  'point_cloud_fusion', 'voxel_grid', 'ndt_localizer', 'universe_ekf_localizer', 'behavior_planner', 'pure_pursuit'],
        'REAR_LIDAR_LOC' : ['virtual_driver_vlp16_rear',    'point_cloud_fusion', 'voxel_grid', 'ndt_localizer', 'universe_ekf_localizer', 'behavior_planner', 'pure_pursuit'],
        'FRONT_LIDAR_DET' : ['virtual_driver_vlp16_front',  'point_cloud_fusion', 'ray_ground_classifier', 'euclidean_clustering', 'multi_object_tracker', 'behavior_planner', 'pure_pursuit'],
        'REAR_LIDAR_DET' :[ 'virtual_driver_vlp16_rear',    'point_cloud_fusion', 'ray_ground_classifier', 'euclidean_clustering', 'multi_object_tracker', 'behavior_planner', 'pure_pursuit'],
        'ODOM' : ['virtual_driver_vehicle_kinematic_state', 'universe_ekf_localizer', 'behavior_planner', 'pure_pursuit']
    }

    profile_data = raw_profile(input_dir, task_cpu_infos) #rosbag ??? ??????, cpu ????????? task ?????? profile data ??????

    # execution_time_data = process_execution_time_orig(profile_data) #profile_data ??? ???????????? task ?????? execution_time ?????? ??????
                                                                    #??????????????? ??? ???????????? ?????? - ????????? ????????? ???????????? ??????

    execution_time_data = process_execution_time_task_to_task(task_cpu_infos, profile_data) #profile_data ??? ???????????? task ?????? execution_time ?????? ??????
                                                                            # ??????????????? ?????? ???????????? ?????? - ?????? ???????????? ???????????? ????????? ??????

    e2e_latency_data    = process_e2e_latency_using_sink(profile_data, e2e_paths) #profile_data ??? ???????????? e2e_path ??? e2e_latency ?????? ??????
    
    execution_time_prob = make_hist(execution_time_data, 'prob') #??? task ??? execution time ?????? ?????? ??????
    e2e_latency_prob = make_hist(e2e_latency_data, 'prob') #??? ?????? ????????? e2e latency ?????? ?????? ??????
    
    make_txt_result(execution_time_prob, output_dir, 'et_prob') #execution time ?????? ????????? txt ????????? ??????    
    make_txt_result(e2e_latency_prob, output_dir, 'e2e_prob') #e2e latency ?????? ????????? txt ????????? ??????

    ###response time###
    # response_time_data  = process_response_time(profile_data) #profile_data ??? ???????????? task ?????? response_time ?????? ??????
    # response_time_prob = make_hist(response_time_data, 'prob') #??? task ??? response time ?????? ?????? ??????
    # make_txt_result(response_time_prob, output_dir, 'rt_prob') #response time ?????? ????????? txt ????????? ??????

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--input', help='input profiled directory name', required=True)
    parser.add_argument('-o', '--output', help='output result directory name', required=True)

    args = parser.parse_args()

    main(args)