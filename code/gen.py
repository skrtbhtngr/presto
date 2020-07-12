import os
import sys
import random
import numpy as np
import matplotlib.pyplot as plt

from scipy.stats import norm
from scipy.stats import uniform
from scipy.stats import poisson
from scipy.stats import beta

from gen_helper import *

SECTORS_IN_GB = 2 ** 21
TIME_SEC = 14400


def get_hotspots(conf):
    vdisk_size = get_size_bytes(conf['vdisk']['size'])
    sector_size = get_size_bytes(conf['sector']['size'])
    total_sectors = int(vdisk_size / sector_size)
    hotspots = conf['hotspots']
    hotspot_pop = []
    hotspot_wt = []

    if type(hotspots) is list:
        hs_count = len(hotspots)
        for hs in hotspots:
            hotspot_wt.append(get_perc(hs[0]))
        assert (sum(hotspot_wt) == 100), "Error: Hotspot I/O percentages do not add up to 100!"

        if len(hotspots[0]) == 3:
            for hs in hotspots:
                hotspot_pop.append([int((hs[1] / 100.0) * total_sectors),
                                    int((hs[2] / 100.0) * total_sectors)])
        else:
            hotspot_vals = []
            for hs in hotspots:
                hotspot_vals.append(get_perc(hs[1]))
            total_disk_perc = sum(hotspot_vals)
            # assert (total_disk_perc <= 90), "Error: Hotspot disk percentages exceed 90!"
            gap_perc = (100 - total_disk_perc) / (hs_count + 1)
            cur_loc = gap_perc
            for hval in hotspot_vals:
                hotspot_pop.append([int((cur_loc / 100.0) * total_sectors),
                                    int(((cur_loc + hval) / 100.0) * total_sectors)])
                cur_loc += hval + gap_perc
    else:
        hs_count = hotspots['count']
        hs_min_size = get_perc(hotspots['span']['min']) / 100.0 * vdisk_size
        hs_max_size = get_perc(hotspots['span']['max']) / 100.0 * vdisk_size
        hs_max_size = min(hs_max_size, 1.0 / hs_count * vdisk_size)
        if 'weights' in hotspots.keys():
            hotspot_wt = [get_perc(wt) for wt in hotspots['weights']]
        else:
            hotspot_wt = [100.0 / hs_count for i in range(hs_count)]
        while True:
            vals = random.sample(range(total_sectors), hs_count * 2)
            vals.sort()
            for i in range(0, int(len(vals)), 2):
                hs_size = (vals[i + 1] - vals[i]) * sector_size
                hotspot_pop.append([vals[i], vals[i + 1]])
                if hs_size < hs_min_size or hs_size > hs_max_size:
                    hotspot_pop = []
                    break
            else:
                break
            continue

    assert (max(hotspot_pop, key=lambda x: x[1])[1] <= total_sectors), \
        "Error: Hotspot range value exceeds disk size!"

    print("Hotspots:")
    print("%4s  %7s  %13s  %21s" % ("%I/O", "[1-100]", "[----GB----]", "[------SECTORS------]"))
    for i in range(len(hotspot_pop)):
        print("%4d  %-3.0f %3.0f  %-6.2f %6.2f  %10d %10d" %
              (hotspot_wt[i], (hotspot_pop[i][0] / total_sectors) * 100, (hotspot_pop[i][1] / total_sectors) * 100,
               hotspot_pop[i][0] / float(SECTORS_IN_GB), hotspot_pop[i][1] / float(SECTORS_IN_GB),
               hotspot_pop[i][0], hotspot_pop[i][1]))

    return hotspot_pop, hotspot_wt


def get_iops(iops, duration):
    iops_vals = [-1]
    if iops['randvar'] == 'normal':
        assert ('mean' in iops.keys() and 'stddev' in iops.keys()), \
            "Error: invalid parameters for normal distribution!"
        while min(iops_vals) < 0:
            iops_vals = norm.rvs(size=duration, loc=iops['mean'], scale=iops['stddev']).astype(int).tolist()
    elif iops['randvar'] == 'uniform':
        assert ('min' in iops.keys() and 'max' in iops.keys()), \
            "Error: invalid parameters for uniform distribution!"
        while min(iops_vals) < 0:
            iops_vals = uniform.rvs(size=duration, loc=iops['min'],
                                    scale=iops['max']).astype(int).tolist()
    elif iops['randvar'] == 'poisson':
        assert ('mu' in iops.keys()), \
            "Error: invalid parameters for poisson distribution!"
        while min(iops_vals) < 0:
            iops_vals = poisson.rvs(size=duration, mu=iops['mu']).astype(int).tolist()
    elif iops['randvar'] == 'beta':
        assert ('alpha' in iops.keys() and 'beta' in iops.keys()
                and 'shift' in iops.keys() and 'scale' in iops.keys()), \
            "Error: invalid parameters for beta distribution!"
        while min(iops_vals) < 0:
            iops_vals = beta.rvs(size=duration, a=iops['alpha'], b=iops['beta'],
                                 loc=iops['shift'], scale=iops['scale']).astype(int).tolist()
    else:
        print("Error: invalid random distribution!")
        exit(0)

    print('Min IOPS: ' + str(min(iops_vals)))
    print('Max IOPS: ' + str(max(iops_vals)))
    return iops_vals


def generate(conf):
    validate(conf)

    name = conf['name']
    wset = conf['set']
    vdisk_id = int(conf['vdisk']['id'])
    vdisk_size = get_size_bytes(conf['vdisk']['size'])
    randseed = int(conf['randseed'])
    duration = get_time_seconds(conf['duration'])
    sector_size = get_size_bytes(conf['sector']['size'])
    sector_cluster_size_min = conf['sector']['cluster']['size']['min']
    sector_cluster_size_max = conf['sector']['cluster']['size']['max']
    sector_cluster_stride_min = conf['sector']['cluster']['stride']['min']
    sector_cluster_stride_max = conf['sector']['cluster']['stride']['max']
    iops = conf['iops']
    readp = get_perc(conf['readp'])

    random.seed(randseed)
    total_sectors = int(vdisk_size / sector_size)

    print('\n\nVDISK ' + str(vdisk_id))

    rw_pop = ['R', 'W']
    rw_wt = [readp, 100 - readp]

    iosize_pop = []
    iosize_wt = []
    for ios in conf['iosizes']:
        iosize_wt.append(get_perc(ios[0]))
        iosize_pop.append(get_size_bytes(ios[1]))
    assert (sum(iosize_wt) == 100), "Error: I/O size percentages do not add up to 100!"

    iops_vals = get_iops(iops, duration)
    num_reqs = sum(iops_vals)

    hotspot_pop, hotspot_wt = get_hotspots(conf)

    sector_cluster_size = 0
    iops_idx = 0
    cur_iops = 0
    ts = 0
    outliers = 0
    if iops_vals[iops_idx] != 0:
        ts_step = 1.0 / iops_vals[iops_idx]
    else:
        ts_step = 1.0

    blocks_r_min = []
    times_r_min = []
    blocks_r_hr = []
    times_r_hr = []
    blocks_o_hr = []
    times_o_hr = []

    fname = 'traces/' + wset + '/' + name + '.csv'
    # if os.path.isfile(fname):
    # sys.stderr.write('File ' + fname + ' already exists!')
    # return
    # pass

    with open(fname, 'w') as file:
        while num_reqs > 0:
            if sector_cluster_size == 0:
                hs = random.choices(population=hotspot_pop, weights=hotspot_wt, k=1)[0]
                sector = random.uniform(hs[0], hs[1])
                while sector == total_sectors or sector % 2 == 1:
                    sector = random.uniform(hs[0], hs[1])
                sector_cluster_size = random.randint(sector_cluster_size_min,
                                                     sector_cluster_size_max)
                rw = random.choices(population=rw_pop, weights=rw_wt, k=1)[0]

            if cur_iops == iops_vals[iops_idx]:
                iops_idx += 1
                ts = iops_idx
                if iops_vals[iops_idx] != 0:
                    ts_step = 1.0 / iops_vals[iops_idx]
                else:
                    ts_step = 1.0
                cur_iops = 0

            iosize = random.choices(population=iosize_pop, weights=iosize_wt, k=1)[0]
            if sector + iosize / sector_size > total_sectors:
                sector_cluster_size = 0
                continue

            # file.write("%c,%d,%ld,%d,%.6f\n" % (rw, vdisk_id, sector, iosize, ts))

            # if sector > hs[1] or sector < hs[0]:
            #     blocks_o_hr.append(sector)
            #     times_o_hr.append(ts)
            #     outliers += 1
            # else:
            if rw == 'R' and ts < 60:
                blocks_r_min.append(sector)
                times_r_min.append(ts)
            if rw == 'R' and ts < 3600:
                blocks_r_hr.append(sector)
                times_r_hr.append(ts)

            if ts > 3600:
                break

            while True:
                sector_stride = random.randint(sector_cluster_stride_min,
                                               sector_cluster_stride_max)
                if sector_stride != 0 and sector_stride % 2 == 1:
                    break

            sector_stride *= random.choices([1, -1], [0.8, 0.2])[0]

            if sector_stride > 0:
                sector_stride += iosize / sector_size
            sector += sector_stride
            if sector > total_sectors or sector < 1:
                sector_cluster_size = 0
            else:
                sector_cluster_size -= 1
            ts += ts_step
            num_reqs -= 1
            cur_iops += 1

    print("Outliers: " + str(outliers))

    plt.clf()
    plt.grid(True, alpha=0.2)
    plt.scatter(times_r_hr, blocks_r_hr, color='b', s=0.5, alpha=0.5)
    # plt.scatter(times_o_hr, blocks_o_hr, color='r', s=0.5, alpha=1)
    plt.xlim([0, 900])
    plt.xticks(np.arange(0, 901, 300), np.arange(0, 16, 5).astype(int))
    plt.ylim([0, total_sectors])
    plt.yticks(np.arange(0, total_sectors + 1, total_sectors / 10),
               np.arange(0, 11, 1))
    for hs in hotspot_pop:
        plt.axhline(y=hs[0], color='r', linestyle='--', linewidth=0.75)
        plt.axhline(y=hs[1], color='r', linestyle='--', linewidth=0.75)
    plt.xlabel('Time (minutes)')
    plt.ylabel('Disk Offset (x' + str(int((vdisk_size / 2 ** 30) / 10)) + ' GB)')
    plt.savefig(fname[:-len('.csv')] + '-wload_char-read-han.png', dpi=300, bbox_inches='tight')

    # plt.clf()
    # plt.grid(True, alpha=0.2)
    # plt.scatter(times_r_min, blocks_r_min, color='b', s=0.25, alpha=0.5)
    # plt.xlim([0, 60])
    # plt.xticks(np.arange(0, 61, 10), np.arange(0, 61, 10).astype(int))
    # plt.ylim([0, total_sectors])
    # plt.yticks(np.arange(0, total_sectors + 1, total_sectors / 10),
    #            np.arange(0, 11, 1))
    # for hs in hotspot_pop:
    #     plt.axhline(y=hs[0], color='r', linestyle='--', linewidth=1)
    #     plt.axhline(y=hs[1], color='r', linestyle='--', linewidth=1)
    # plt.xlabel('Time (seconds)')
    # plt.ylabel('Disk Offset (x' + str(int((vdisk_size / 2 ** 30) / 10)) + ' GB)')
    # plt.savefig(fname[:-len('.csv')] + '-wload_char-read-minute.png', dpi=600, bbox_inches='tight')

    # iops_avg = []
    # time_scale_sec = 600.0
    # for i in range(0, len(iops_vals), int(time_scale_sec)):
    #     iops_avg.append(sum(iops_vals[i:i + int(time_scale_sec)]) / time_scale_sec)
    # plt.clf()
    # plt.grid(True, alpha=0.2)
    # plt.plot(iops_avg, color='g', alpha=0.5)
    # plt.xlim([0, TIME_SEC / time_scale_sec])
    # plt.xticks(np.arange(0, (TIME_SEC / time_scale_sec), 1),
    #            np.arange(0, (TIME_SEC / time_scale_sec), 1).astype(int))
    # plt.ylim([0, max(iops_vals)])
    # # plt.yticks(np.arange(0, total_sectors + 1, total_sectors / 10),
    # #            np.arange(0, 11, 1))
    # plt.xlabel('Time (x' + str(int(time_scale_sec / 60)) + ' mins)')
    # plt.ylabel('Average IOPS')
    # plt.savefig(fname[:-len('.csv')] + '-wload_char-iops.png', dpi=300, bbox_inches='tight')
