import matplotlib.pyplot as plt
import numpy as np
import csv
import os

# basepath = "/Users/skrtbhtngr/CLionProjects/presto/"
basepath = "/home/skrtbhtngr/CLionProjects/presto/"

TYPE = 0
VDISK = 1
SECTOR = 2
SIZE = 3
TIMESTAMP = 4

RUNTIME_SEC = 14400

SIZE_HM1_OBJ = 1400
SIZE_HM3_OBJ = 27648


def get_size(hm1, hm3):
    return hm1 * 1400 + hm3 * 27648


def diff(vdisks, wload):
    for vdisk in vdisks[wload]:
        with open(vdisk[2]) as tfile:
            print(vdisk[2])
            lines = csv.reader(tfile)
            prev = 0
            for line in lines:
                if prev == 0:
                    prev = int(line[2])
                    continue
                if 0 < prev - int(line[2]) < 10000:
                    print(prev, int(line[2]))
                    print(prev - int(line[2]))
                prev = int(line[2])


def gen_block_access_pattern(vdisks, wload):
    for vdisk in vdisks:
        blocks_all = []
        times_all = []
        blocks_r = []
        blocks_w = []
        times_r = []
        times_w = []

        with open(vdisk[2]) as tfile:
            lines = csv.reader(tfile)
            for line in lines:
                blocks_all.append(int(line[2]))
                times_all.append(float(line[4]))
                if str(line[0])[0] == 'R':
                    blocks_r.append(int(line[2]))
                    times_r.append(float(line[4]))
                else:
                    blocks_w.append(int(line[2]))
                    times_w.append(float(line[4]))

        plt.clf()
        plt.grid(True, alpha=0.3)
        plt.scatter(times_all, blocks_all, color='b', s=1, alpha=0.5)
        plt.xlim([0, RUNTIME_SEC])
        plt.xticks(np.arange(0, RUNTIME_SEC + 1, 3600), np.arange(0, RUNTIME_SEC / 3600 + 1, 1).astype(int))
        plt.ylim([0, vdisk[1] * 2 ** 21])
        plt.yticks(np.arange(0, vdisk[1] * 2 ** 21 + 1, (vdisk[1] * 2 ** 21) / 10),
                   np.arange(0, 11, 1))
        plt.xlabel('Time (hours)')
        plt.ylabel('Disk Offset (x' + str(round(vdisk[1] / 10, 2)) + ' GB)')
        # plt.show()
        plt.savefig('traces/' + wload + '/' + vdisk[2].split('/')[1] + '-blocks.png', dpi=300, bbox_inches='tight')

        # plt.clf()
        # plt.grid(True, alpha=0.3)
        # w = plt.scatter(times_w, blocks_w, color='r', s=1, alpha=0.5)
        # plt.xlim([0, RUNTIME_SEC])
        # plt.xticks(np.arange(0, RUNTIME_SEC + 1, 3600), np.arange(0, RUNTIME_SEC / 3600 + 1, 1).astype(int))
        # plt.ylim([0, vdisk[1] * 2 ** 21])
        # plt.yticks(np.arange(0, vdisk[1] * 2 ** 21 + 1, (vdisk[1] * 2 ** 21) / 10),
        #            np.arange(0, 11, 1))
        # plt.xlabel('Time (hours)')
        # plt.ylabel('Disk Offset (x' + str(round(vdisk[1] / 10, 2)) + ' GB)')
        # plt.savefig(vdisk[2].split(".csv")[0] + '-wload_char-write.png', dpi=300, bbox_inches='tight')
        #
        # plt.clf()
        # plt.grid(True, alpha=0.3)
        # r = plt.scatter(times_r, blocks_r, color='b', s=1, alpha=0.5)
        # plt.xlim([0, RUNTIME_SEC])
        # plt.xticks(np.arange(0, RUNTIME_SEC + 1, 3600), np.arange(0, RUNTIME_SEC / 3600 + 1, 1).astype(int))
        # plt.ylim([0, vdisk[1] * 2 ** 21])
        # plt.yticks(np.arange(0, vdisk[1] * 2 ** 21 + 1, (vdisk[1] * 2 ** 21) / 10),
        #            np.arange(0, 11, 1))
        # plt.xlabel('Time (hours)')
        # plt.ylabel('Disk Offset (x' + str(round(vdisk[1] / 10, 2)) + ' GB)')
        #
        # # l = plt.legend((r, w), ('Read', 'Write'), loc='upper right', prop={'size': 10})
        # # for lh in l.legendHandles:
        # #     lh.set_alpha(1)
        # #     lh.set_sizes([8])
        # plt.savefig(vdisk[2].split(".csv")[0] + '-wload_char-read.png', dpi=300, bbox_inches='tight')


def gen_vdisk_iops(vdisks, wload, till):
    avg = []
    # idx = np.arange(len(vdisks))
    ids = []
    for vdisk in vdisks:
        iops = []
        # ids.append(int(vdisk[2]))
        # ids.append(vdisk[0].split('/')[-1].split('.csv')[0])
        with open(vdisk[2]) as tfile:
            tm = 0
            count = 0
            maxc = 0
            lines = csv.reader(tfile)
            for line in lines:
                ts = float(line[TIMESTAMP])
                # print(ts, tm)
                # if ts > RUNTIME_SEC:
                #     break
                if ts >= till:
                    break
                if ts >= tm + 1:
                    for i in range(0, (int(ts) - (tm + 1))):
                        iops.append(0)
                    iops.append(count)
                    if count > maxc:
                        maxc = count
                    count = 1
                    tm = int(ts)
                else:
                    count += 1
            iops.append(count)
            # plt.plot(iops, color='purple')
            # plt.xlim([0, TIME_SEC])
            # plt.xticks(np.arange(0, 43201, 3600), np.arange(0, 13, 1))
            # plt.xlabel('Time (hours)')
            # plt.ylim([0, maxc])
            # plt.ylabel('I/O Requests')
            # plt.savefig(vdisk[0].split(".csv")[0] + '-wload_char-iops.png', dpi=300, bbox_inches='tight')
        avg.append(sum(iops) / len(iops))
        # print(len(iops))

    # print(ids)
    for a in avg:
        print(round(a, 2))
    # plt.clf()
    # plt.bar(idx, avg, tick_label=ids)
    # plt.xticks(rotation=45)
    # plt.xlabel('vDisk IDs')
    # plt.ylabel('Average IOPS')
    # plt.ylabel('Average IOPS (12 hrs)')
    # plt.show()
    # plt.savefig(basepath + "traces/" + wload + '/vdisk-iops.png', dpi=300, bbox_inches='tight')


def gen_wss_objs(wload, size, vdisk_id, wss_type):
    wssfile = basepath + "results/exp4/" + wload + "/VDISK-" + str(vdisk_id) + "/" \
              + str(size) + "M-OBJ.csv"
    wss_sec = []
    wss_hm1 = []
    wss_hm3 = []
    with open(wssfile) as wssfile:
        lines = csv.reader(wssfile)
        for line in lines:
            wss_sec.append(int(line[0]))
            wss_hm1.append(int(line[1]))
            wss_hm3.append(int(line[2]))
    plt.clf()
    plt.title('WSS' + wss_type + ' = ' + str(size) + 'MB')
    plt.plot(wss_sec, wss_hm1, color='b', linewidth=1.3)
    plt.xlabel('Time (minutes)')
    plt.xlim([0, RUNTIME_SEC])
    plt.xticks(np.arange(0, RUNTIME_SEC + 1, 600), np.arange(0, RUNTIME_SEC / 60 + 1, 10).astype(int))
    plt.ylabel('HM1 Objects')
    wss80_hm1_max = max(wss_hm1)
    for i in range(len(wss_hm1)):
        if wss_hm1[i] >= wss80_hm1_max:
            break
    plt.axvline(x=i, color='r', linestyle='--', linewidth=1)
    plt.savefig(basepath + "results/exp4/" + wload + "/VDISK-" + str(vdisk_id) + "/WSS" + wss_type + "-HM1.png",
                dpi=600, bbox_inches='tight')

    plt.clf()
    plt.title('WSS' + wss_type + ' = ' + str(size) + 'MB')
    plt.plot(wss_sec, wss_hm3, color='b', linewidth=1.3)
    plt.xlabel('Time (minutes)')
    plt.xlim([0, RUNTIME_SEC])
    plt.xticks(np.arange(0, RUNTIME_SEC + 1, 600), np.arange(0, RUNTIME_SEC / 60 + 1, 10).astype(int))
    plt.ylabel('HM3 Objects')
    wss_hm3_max = max(wss_hm3)
    for i in range(len(wss_hm3)):
        if wss_hm3[i] >= wss_hm3_max:
            break
    plt.axvline(x=i, color='r', linestyle='--', linewidth=1)
    plt.savefig(basepath + "results/exp4/" + wload + "/VDISK-" + str(vdisk_id) + "/WSS" + wss_type + "-HM3.png",
                dpi=600, bbox_inches='tight')

    wss_sum = [wss_hm1[i] + wss_hm3[i] for i in range(len(wss_hm1))]
    plt.clf()
    plt.title('WSS' + wss_type + ' = ' + str(size) + 'MB')
    plt.plot(wss_sec, wss_sum,
             color='b', linewidth=1.3)
    plt.xlabel('Time (minutes)')
    plt.xlim([0, RUNTIME_SEC])
    plt.xticks(np.arange(0, RUNTIME_SEC + 1, 600), np.arange(0, RUNTIME_SEC / 60 + 1, 10).astype(int))
    plt.ylabel('Total Objects (HM1+HM3)')
    wss_sum_max = max(wss_sum)
    for i in range(len(wss_sum)):
        if wss_sum[i] >= wss_sum_max:
            break
    plt.axvline(x=i, color='r', linestyle='--', linewidth=1)
    plt.savefig(basepath + "results/exp4/" + wload + "/VDISK-" + str(vdisk_id) + "/WSS" + wss_type + "-SUM.png",
                dpi=600, bbox_inches='tight')


def gen_vdisk_wss(vdisks, wload):
    sizes = [1, 5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
             125, 150, 175, 200, 225, 250, 275, 300, 325, 350, 375, 400, 425, 450, 475, 500,
             550, 600, 650, 700, 750, 800, 850, 900, 950, 1000, 1100, 1200, 1300, 1400, 1500,
             1600, 1700, 1800, 1900, 2000, 2100, 2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900,
             3000]

    wss80 = []
    wss90 = []
    time_to_wss80 = []
    time_to_wss90 = []
    time_to_hr80 = []
    time_to_hr90 = []
    highest_hr = []
    lastsize = []
    lowest_time_to_hr80 = []

    for vdisk in vdisks:
        sum = 0
        numl = 0
        hrfname = ''
        for size in reversed(sizes):
            hrfname = basepath + "results/exp4/" + wload + "/VDISK-" + str(vdisk[0]) + "/" + str(size) + "M-HR.csv"
            if os.path.isfile(hrfname):
                lastsize.append(size)
                break
        with open(hrfname) as resfile:
            lines = csv.reader(resfile)
            for line in lines:
                sum += float(line[1])
                numl += 1
            avg = sum / float(numl)
            highest_hr.append([avg, 0])

    for i, vdisk in enumerate(vdisks):
        print("VDISK: " + str(vdisk))
        if not os.path.isdir(basepath + "results/exp4/" + wload + "/VDISK-" + str(vdisk[0])):
            print('No WSS result files for VDISK ' + str(vdisk[0]))
            break
        flag80 = flag90 = flagh = True
        lowest_time_to_hr80.append([RUNTIME_SEC, 0])
        for size in sizes:
            sum = 0
            numl = 0
            hrfname = basepath + "results/exp4/" + wload + "/VDISK-" + str(vdisk[0]) + "/" + str(size) + "M-HR.csv"
            if not os.path.isfile(hrfname):
                break
            with open(hrfname) as resfile:
                lines = csv.reader(resfile)
                for line in lines:
                    sum += float(line[1])
                    numl += 1
                    if float(line[1]) >= 0.80 and float(line[0]) < lowest_time_to_hr80[i][0]:
                        lowest_time_to_hr80[i][0] = float(line[0])
                        lowest_time_to_hr80[i][1] = size
            avg = sum / float(numl)

            if avg >= highest_hr[i][0] and flagh:
                highest_hr[i][1] = size
                flagh = False

            if avg >= 0.80 and flag80:
                flag80 = False
                wss80.append(size)
                objfname = basepath + "results/exp4/" + wload + "/VDISK-" + str(vdisk[0]) + "/" + \
                           str(size) + "M-OBJ.csv"
                with open(objfname) as objfile:
                    lines = list(csv.reader(objfile))
                    flag80tt = True
                    for j, line in enumerate(lines):
                        if j == 0:
                            continue
                        objsize = get_size(int(line[1]), int(line[2]))
                        if objsize == size * 2 ** 20 or objsize + SIZE_HM1_OBJ > size * 2 ** 20 \
                                or objsize + SIZE_HM3_OBJ > size * 2 ** 20:
                            time_to_wss80.append(int(line[0]))
                            flag80tt = False
                            break
                    if flag80tt:
                        objsize_p = 0
                        for j, line in enumerate(reversed(lines)):
                            objsize = get_size(int(line[1]), int(line[2]))
                            if objsize > objsize_p:
                                objsize_p = objsize
                            elif objsize < objsize_p:
                                time_to_wss80.append(int(lines[j + 1][0]))
                                flag80tt = False
                                break

                with open(hrfname) as hrfile:
                    lines = csv.reader(hrfile)
                    for j, line in enumerate(lines):
                        if j == 0:
                            continue
                        if float(line[1]) >= 0.80:
                            time_to_hr80.append(int(line[0]))
                            break
                # gen_wss_objs(wload, size, vdisk[0], "80")

            if avg >= 0.90 and flag90:
                flag90 = False
                wss90.append(size)
                objfname = basepath + "results/exp4/" + wload + "/VDISK-" + str(vdisk[0]) + "/" + \
                           str(size) + "M-OBJ.csv"
                with open(objfname) as objfile:
                    lines = list(csv.reader(objfile))
                    flag90tt = True
                    for j, line in enumerate(lines):
                        if j == 0:
                            continue
                        objsize = get_size(int(line[1]), int(line[2]))
                        if objsize == size * 2 ** 20 or objsize + SIZE_HM1_OBJ > size * 2 ** 20 \
                                or objsize + SIZE_HM3_OBJ > size * 2 ** 20:
                            time_to_wss90.append(int(line[0]))
                            flag90tt = False
                            break
                    if flag90tt:
                        objsize_p = 0
                        for j, line in enumerate(reversed(lines)):
                            objsize = get_size(int(line[1]), int(line[2]))
                            if objsize > objsize_p:
                                objsize_p = objsize
                            elif objsize < objsize_p:
                                time_to_wss90.append(int(lines[j + 1][0]))
                                flag90tt = False
                                break

                with open(hrfname) as hrfile:
                    lines = csv.reader(hrfile)
                    for j, line in enumerate(lines):
                        if j == 0:
                            continue
                        if float(line[1]) >= 0.90:
                            time_to_hr90.append(int(line[0]))
                            break
                # gen_wss_objs(wload, size, vdisk[0], "90")

            if size == lastsize[i]:
                if flag80:
                    wss80.append(0)
                    time_to_wss80.append(RUNTIME_SEC)
                    time_to_hr80.append(RUNTIME_SEC)
                if flag90:
                    wss90.append(0)
                    time_to_wss90.append(RUNTIME_SEC)
                    time_to_hr90.append(RUNTIME_SEC)
                if flagh:
                    highest_hr[i][1] = sizes[-1]

    if len(wss80) != len(wss90) or len(time_to_wss80) != len(time_to_wss90) or len(wss80) != len(time_to_wss80) or \
            len(wss80) != len(lastsize) or len(time_to_hr80) != len(time_to_hr90):
        print("We have a problem!")
        # exit(1)

    wssfile = open(basepath + "results/exp4/" + wload + "/wss.txt", 'w')
    wssfile.write("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n" %
                  ("vDisk ID", "wss80", "Time to wss80", "Time to hr80", "wss90", "Time to wss90", "Time to hr90",
                   "Highest HR", "Lowest Cache Size",
                   "Fastest hr80", "Fastest hr80 Cache Size"))
    for i in range(len(vdisks)):
        wssfile.write("%s,%d,%d,%d,%d,%d,%d,%.3lf,%d,%d,%d\n" % (
            vdisks[i][2][len(basepath) + len("traces/"):-len(".csv")], wss80[i], time_to_wss80[i], time_to_hr80[i],
            wss90[i], time_to_wss90[i], time_to_hr90[i], highest_hr[i][0], highest_hr[i][1], lowest_time_to_hr80[i][0],
            lowest_time_to_hr80[i][1]))


def gen_block_access_freq(vdisks):
    for vdisk in vdisks:
        blocks = []
        for i in range(vdisk[1] * 2097152):
            blocks.append(0)

        maxf = 0
        with open(vdisk[0]) as tfile:
            lines = csv.reader(tfile)
            for line in lines:
                block = int(line[2])
                blocks[block] += 1
            for block in blocks:
                if block > maxf:
                    maxf = block

        plt.plot(blocks, color='g')
        plt.xlim([0, vdisk[1] * 2 ** 21])
        plt.xticks(np.arange(0, vdisk[1] * 2 ** 21 + 1, (vdisk[1] * 2 ** 21) / 10),
                   np.arange(0, 11, 1))
        plt.xlabel('Disk Offset (x' + str(int(vdisk[1] / 10)) + ' GB)')
        plt.ylim([0, maxf])
        plt.ylabel('Access Frequency')
        plt.savefig(vdisk[0].split(".csv")[0] + '-wload_char-hist.png', dpi=300, bbox_inches='tight')


def main():
    # plt.rcParams.update({'font.size': 14})
    # plt.ticklabel_format(useOffset=False, style='plain')

    wload = "migwload_3gb"
    vdisks = []

    with open('traces/' + wload + '.csv') as file:
        lines = csv.reader(file)
        for line in lines:
            vdisks.append([int(line[0]), int(line[1]), basepath + 'traces/' + line[2]])

    # gen_block_access_pattern(vdisks, wload)

    # gen_block_access_freq(vdisks, wload)

    # for wload in vdisks.keys():
    gen_vdisk_iops(vdisks, wload, 5400)

    # for wload in vdisks.keys():
    # gen_vdisk_wss(vdisks, wload)

    # restructure_csv_traces(vdisks)

    # for wload in vdisks.keys():
    #     diff(vdisks, wload)


if __name__ == "__main__":
    main()
