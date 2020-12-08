import json
import os
import time
import warnings
from collections import deque
from math import gcd
from multiprocessing import Process, Queue

from ai2thor.controller import BFSController
from datasets.offline_controller_with_small_rotation import ExhaustiveBFSController

def noop(self):
    pass

BFSController.lock_release = noop
BFSController.unlock_release = noop
BFSController.prune_releases = noop

def search_and_save(in_queue):
    while not in_queue.empty():
        try:
            scene_name = in_queue.get(timeout=3)
        except:
            return
        # scene_name = in_queue
        c = None
        try:
            out_dir = os.path.join('./data/2.4.22_plans/', scene_name)
            if not os.path.exists(out_dir):
                os.mkdir(out_dir)

            print('starting:', scene_name)
            c = ExhaustiveBFSController(
                grid_size=0.25,
                fov=90.0,
                grid_file=os.path.join(out_dir, 'grid.json'),
                graph_file=os.path.join(out_dir, 'graph.json'),
                metadata_file=os.path.join(out_dir, 'metadata.json'),
                images_file=os.path.join(out_dir, 'images.hdf5'),
                depth_file=os.path.join(out_dir, 'depth.hdf5'),
                grid_assumption=False)
            c.start()
            c.search_all_closed(scene_name)
            c.stop()
        except AssertionError as e:
            print('Error is', e)
            print('Error in scene {}'.format(scene_name))
            if c is not None:
                c.stop()
            continue

def extract_visible_objects(data_dir, scenes):
    for scene in scenes:
        try:
            with open('{}/{}/metadata.json'.format(data_dir, scene), 'r') as f:
                metadata_list = json.load(f)

            visible_map = {}
            for k in metadata_list:
                metadata = metadata_list[k]
                for obj in metadata['objects']:
                    if obj['visible']:
                        objId = obj['objectId']
                        if objId not in visible_map:
                            visible_map[objId] = []
                        visible_map[objId].append(k)
            
            with open('{}/{}/visible_object_map.json'.format(data_dir, scene), 'w') as f:
                json.dump(visible_map, f)
        except Exception as e:
            print(scene, e)


def main():

    num_processes = 6
    
    queue = Queue()
    scene_names = []
    for i in range(0,4):
        for j in range(30):
            if i == 0:
                scene_names.append("FloorPlan" + str(j + 1))
            else:
                scene_names.append("FloorPlan" + str(i + 1) + '%02d' % (j + 1))

    if True:
        for x in scene_names:
            queue.put(x)

        processes = []
        for i in range(num_processes):
            p = Process(target=search_and_save, args=(queue,))
            p.start()
            processes.append(p)

        for p in processes:
            p.join()
    else:
        search_and_save("FloorPlan30")

    extract_visible_objects('./data/2.4.22_plans/', scene_names)

if __name__ == "__main__":
    main()