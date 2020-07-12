import matplotlib.pyplot as plt
import matplotlib
import csv
import numpy as np
import os
import threading

RUNTIME_SEC = 14400


def main(wload, target_cache_size):
    plt.rcParams.update({'font.size': 14})
    plt.rcParams.update({'mathtext.default': 'regular'})

    heuristics = ['K_FREQ_MEM', 'K_RECN_MEM', 'K_FRERECN_MEM']
    hname = ['k-Frequent', 'k-Recent', 'k-Frerecent']  # constrained
    snaprates = ['10000', '50000', '100000']
    sname = ['10 K', '50 K', '100 K']
    # scores = ['100%', '50%', '33%', '25%', '20%', '1', '2', '3', '4', '5', '1%', '5%', '10%', '15%', '20%']
    # scname = ['100\%', '50\%', '33\%', '25\%', '20\%', '1', '2', '3', '4', '5', '1\%', '5\%', '10\%', '15\%', '20\%']
    mlimits = ['5%', '10%', '25%', '50%', '75%', '100%']
    mname = ['5%', '10%', '25%', '50%', '75%', '100%']
    # mlimits = ['75%', '100%']
    # mname = ['75%', '100%']
    frerern_func = ['LNR', 'QDR']
    func_name = ['Linear', 'Quadratic']
    prewarmrates = ['0MBps', '500MBps', '100MBps', '50MBps']
    pratenames = ['∞', '500 MBps', '100 MBps', '50 MBps']

    moment = 5400
    wss_window = 600
    wss_stride = 300
    hr_windows = [5, 15, 30]

    # wload = 'scr_merg'  # <<--------------------
    # target_cache_size = '64 MiB'  # <<--------------------
    wload_file = 'traces/' + wload + '.csv'
    with open(wload_file) as wfile:
        vdisks = list(csv.reader(wfile))

    ##################################################################################################################
    ######## NOFAIL
    ##################################################################################################################

    basedir_nop = 'results/exp2/' + wload + '.nofail/10000/'

    x = []
    hr_wof_cache = []
    with open(basedir_nop + 'COLD-ALL-HR.csv') as nopfile:
        lines = list(csv.reader(nopfile))
    for line in lines:
        x.append(int(line[0]))
        hr_wof_cache.append(float(line[1]))

    hrs_wof_vdisk = []
    wof_misses = [0] * len(x)
    for vd in range(len(vdisks)):
        hr_vdisk = []
        with open(basedir_nop + 'COLD-VDISK-' + vdisks[vd][0] + '-HR.csv') as nopfile:
            lines = list(csv.reader(nopfile))
        for line in lines:
            hr_vdisk.append(float(line[1]))
        hrs_wof_vdisk.append(hr_vdisk)

        with open(basedir_nop + 'COLD-VDISK-' + vdisks[vd][0] + '-OBJ.csv') as nopfile:
            lines = list(csv.reader(nopfile))
        for ln in range(len(lines)):
            wof_misses[ln] += int(lines[ln][4]) + int(lines[ln][5])

    ##################################################################################################################
    ######## COLD
    ##################################################################################################################

    basedir_nop = 'results/exp2/' + wload + '/' + snaprates[0] + '/'

    x = []
    hr_wop_cache = []
    hr_val_before_fail = 0
    tt_hr_val_before_fail_wop = 14400
    avg_hr_wop_after_fail = []
    inst_hr_wop = 0
    with open(basedir_nop + 'COLD-ALL-HR.csv') as nopfile:
        lines = list(csv.reader(nopfile))
    for line in lines:
        x.append(int(line[0]))
        hr_wop_cache.append(float(line[1]))
        if x[-1] == moment - 1:
            hr_val_before_fail = round(hr_wop_cache[-1], 3)
    for n in range(moment, len(hr_wop_cache)):
        if hr_wop_cache[n] >= hr_val_before_fail:
            tt_hr_val_before_fail_wop = n
            break

    for nn in hr_windows:
        suml = 0
        numl = 0
        for n in range(moment, moment + nn * 60):
            suml += hr_wop_cache[n]
            numl += 1
        avg_hr_wop_after_fail.append(round(suml / numl, 2))
    inst_hr_wop = hr_wop_cache[moment]

    hrs_wop_vdisk = []
    wss_before_failure_vdisk = []
    wop_misses = [0] * len(x)
    for vd in range(len(vdisks)):
        hr_vdisk = []
        with open(basedir_nop + 'COLD-VDISK-' + vdisks[vd][0] + '-HR.csv') as nopfile:
            lines = list(csv.reader(nopfile))
        for line in lines:
            hr_vdisk.append(float(line[1]))
        hrs_wop_vdisk.append(hr_vdisk)

        with open(basedir_nop + 'COLD-VDISK-' + vdisks[vd][0] + '-WSS-' + str(wss_window) + '-' +
                  str(wss_stride) + '.csv') as nopfile:
            lines = list(csv.reader(nopfile))
        wss_step = int(wss_window / wss_stride)
        wss_epoch = int((moment / 60) / (wss_window / 60)) * wss_step - 1
        wss_before_failure_vdisk.append(round(int(lines[wss_epoch][1]) / 1048576, 2))

        with open(basedir_nop + 'COLD-VDISK-' + vdisks[vd][0] + '-OBJ.csv') as nopfile:
            lines = list(csv.reader(nopfile))
        for ln in range(len(lines)):
            wop_misses[ln] += int(lines[ln][4]) + int(lines[ln][5])

    for n in wss_before_failure_vdisk:
        print(str(n) + ' ', end='')
    print()

    ##################################################################################################################
    ######## PREWARMED
    ##################################################################################################################

    for ssr in range(len(snaprates)):
        break
        for mlim in range(len(mlimits)):
            for prate in range(len(prewarmrates)):
                for hrstc in range(len(heuristics)):
                    for ffn in range(len(frerern_func) if hrstc == 2 else 1):

                        basedir_nop = 'results/exp2/' + wload + '/' + snaprates[ssr] + '/'
                        basedir_pre = basedir_nop + mlimits[mlim] + '-' + prewarmrates[prate] + '-' + \
                                      heuristics[hrstc] + '-' + ((frerern_func[ffn] + '-') if (hrstc == 2) else '') + \
                                      'x/'
                        # print(basedir_pre[len(basedir_nop):-1])
                        hr_wp_cache = []
                        tt_hr_val_before_fail_wp = 14400
                        avg_hr_wp_after_fail = []
                        with open(basedir_pre + 'ALL-HR.csv') as prefile:
                            lines = list(csv.reader(prefile))
                        for line in lines:
                            hr_wp_cache.append(float(line[1]))

                        for n in range(moment, len(hr_wp_cache)):
                            if hr_wp_cache[n] >= hr_val_before_fail:
                                tt_hr_val_before_fail_wp = n
                                break
                        for nn in hr_windows:
                            suml = 0
                            numl = 0
                            for n in range(moment, moment + nn * 60):
                                suml += hr_wp_cache[n]
                                numl += 1
                            avg_hr_wp_after_fail.append(round(suml / numl, 2))
                        inst_hr_wp = hr_wp_cache[moment]

                        gain = round(tt_hr_val_before_fail_wop / tt_hr_val_before_fail_wp, 2)
                        diff = round((tt_hr_val_before_fail_wop - tt_hr_val_before_fail_wp) / 60, 2)

                        objs_in_preset_vdisk = []
                        # hm1_objs_vdisk = []

                        with open(basedir_pre + str(moment) + 's-SHARE.csv') as prefile:
                            lines = list(csv.reader(prefile))
                        for line in lines:
                            objs_in_preset_vdisk.append(int(line[2]))

                        wp_misses = [0] * len(x)
                        hrs_wp_vdisk = []
                        for vd in range(len(vdisks)):
                            with open(basedir_pre + 'VDISK-' + vdisks[vd][0] + '-OBJ.csv') as prefile:
                                lines = list(csv.reader(prefile))
                            for ln in range(len(lines)):
                                wp_misses[ln] += int(lines[ln][4]) + int(lines[ln][5])

                            hr_vdisk = []
                            with open(basedir_pre + 'VDISK-' + vdisks[vd][0] + '-HR.csv') as prefile:
                                lines = list(csv.reader(prefile))
                            for line in lines:
                                hr_vdisk.append(float(line[1]))
                            hrs_wp_vdisk.append(hr_vdisk)

                        pre_count = 0
                        total_prewarmed_vdisk = [0] * len(vdisks)
                        for n in range(moment, len(hr_wp_cache)):
                            fname = basedir_pre + str(n) + 's-SHARE.csv'
                            if os.path.isfile(fname):
                                pre_count += 1
                                with open(fname) as prefile:
                                    lines = list(csv.reader(prefile))
                                for nn in range(len(lines)):
                                    total_prewarmed_vdisk[nn] += int(lines[nn][1])
                            else:
                                break
                        total_prewarmed_vdisk = [round(x / 1048576, 2) for x in total_prewarmed_vdisk]

                        total_prewarmed_sec = []
                        for n in range(moment, len(hr_wp_cache)):
                            fname = basedir_pre + str(n) + 's-SHARE.csv'
                            if os.path.isfile(fname):
                                with open(fname) as prefile:
                                    lines = list(csv.reader(prefile))
                                total_prewarmed = 0
                                for nn in range(len(lines)):
                                    total_prewarmed += int(lines[nn][1])
                                total_prewarmed_sec.append(total_prewarmed)
                            else:
                                break

                        usage_wp_cache = []
                        total_vdiskio_sec = []
                        with open(basedir_pre + 'ALL-USAGE.csv') as prefile:
                            lines = list(csv.reader(prefile))
                        for line in lines:
                            usage_wp_cache.append(float(line[1]))
                        for n in range(len(total_prewarmed_sec)):
                            total_vdiskio_sec.append(usage_wp_cache[moment + n] - total_prewarmed_sec[n])

                        # print(total_prewarmed_sec)
                        # print(total_vdiskio_sec)
                        # plt.plot(x[moment:moment + len(total_prewarmed_sec)], total_prewarmed_sec, color='r')
                        # plt.plot(x[moment:moment + len(total_prewarmed_sec)], total_vdiskio_sec, color='g')
                        # plt.show()
                        # exit(0)

                        print(
                            "%s,%s,%s,%s,%s,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%.2lf,%d" %
                            (sname[ssr], mname[mlim], pratenames[prate], hname[hrstc],
                             (func_name[ffn] if hrstc == 2 else '-'), hr_val_before_fail,
                             inst_hr_wop, inst_hr_wp,
                             tt_hr_val_before_fail_wop / 60, tt_hr_val_before_fail_wp / 60,
                             gain, diff,
                             avg_hr_wop_after_fail[0], avg_hr_wp_after_fail[0], avg_hr_wop_after_fail[1],
                             avg_hr_wp_after_fail[1], avg_hr_wop_after_fail[2], avg_hr_wp_after_fail[2],
                             pre_count),
                            end=',')
                        for n in range(len(total_prewarmed_vdisk)):
                            print("%.2lf" % (total_prewarmed_vdisk[n]), end=',')
                        print()

                        colors = ['tab:blue', 'tab:orange', 'tab:green', 'tab:purple', 'tab:cyan', 'tab:olive',
                                  'tab:brown', 'tab:pink']
                        classes = []
                        for vd in range(len(vdisks)):
                            classes.append('vDisk ' + str(vd + 1))

                        iops = []
                        if wload == 'wload_6gb':
                            iops = [99.16, 24.56, 49.59, 199.25, 99.54, 149.74]
                        elif wload == 'wload_4gb':
                            iops = [49.46, 24.61, 100.03, 149.27, 199.4, 149.92]
                        elif wload == 'rreal.4hr':
                            iops = [5.18, 15.15, 3.03, 2.22, 2.57, 1.81, 0.89, 0.39]
                        elif wload == 'scr_merg':
                            iops = [3.53, 0.12, 51.17, 55.22, 362.62, 0.88, 212.14, 11.97]

                        fsize_iops = [11 if x >= 200 else (
                            10 if x >= 150 else (9 if x >= 100 else (8 if x >= 50 else (7 if x >= 25 else 6))))
                                      for x in iops]
                        sc_iops = [4 * x for x in iops]
                        #############################################################################################
                        # plt.clf()
                        # fig, ax = plt.subplots()
                        # plt.grid(b=True, color='grey', linewidth=0.5, alpha=0.25, zorder=1)
                        # for vd in range(len(vdisks)):
                        #     plt.scatter(wss_before_failure_vdisk[vd], total_prewarmed_vdisk[vd], s=50,
                        #                 c=colors[vd], label=classes[vd], zorder=2 + vd)
                        # plt.legend(bbox_to_anchor=(1.01, 1.0), loc='upper left', fancybox=True, prop={'size': 10})
                        # plt.xlabel('WSS before failure (MB)')
                        # plt.ylabel('Prewarm Set Size (MB)')
                        # plt.savefig(basedir_pre[:-1] + '-vdisk-corr-wss-share.png', dpi=300, bbox_inches='tight')
                        # plt.close(fig)
                        # plt.clf()
                        # fig, ax = plt.subplots()
                        # plt.grid(b=True, color='grey', linewidth=0.5, alpha=0.25, zorder=1)
                        # for vd in range(len(vdisks)):
                        #     plt.scatter(iops[vd], total_prewarmed_vdisk[vd], s=50,
                        #                 c=colors[vd], label=classes[vd], zorder=2 + vd)
                        # plt.legend(bbox_to_anchor=(1.01, 1.0), loc='upper left', fancybox=True, prop={'size': 10})
                        # plt.xlabel('Avg. IOPS')
                        # plt.ylabel('Prewarm Set Size (MB)')
                        # plt.savefig(basedir_pre[:-1] + '-vdisk-corr-iops-share.png', dpi=300, bbox_inches='tight')
                        # plt.close(fig)
                        # plt.clf()
                        # fig, ax = plt.subplots()
                        # plt.grid(b=True, color='grey', linewidth=0.5, alpha=0.25, zorder=1)
                        # for vd in range(len(vdisks)):
                        #     plt.scatter(iops[vd], wss_before_failure_vdisk[vd], s=50,
                        #                 c=colors[vd], label=classes[vd], zorder=2 + vd)
                        # plt.legend(bbox_to_anchor=(1.01, 1.0), loc='upper left', fancybox=True, prop={'size': 10})
                        # plt.xlabel('Avg. IOPS')
                        # plt.ylabel('WSS before failure (MB)')
                        # plt.savefig(basedir_pre[:-1] + '-vdisk-corr-iops-wss.png', dpi=300, bbox_inches='tight')
                        # plt.close(fig)
                        #############################################################################################
                        # plt.clf()
                        # for vd in range(len(vdisks)):
                        #     plt.plot(x, hrs_wp_vdisk[vd], color=colors[vd], linewidth=1, label='vDisk ' + str(vd + 1),
                        #              alpha=1)
                        #     plt.plot(x, hrs_wop_vdisk[vd], color=colors[vd], linewidth=0.6, linestyle='--', alpha=0.5)
                        # plt.legend(bbox_to_anchor=(1.03, 1.0), loc='upper left', fancybox=True, prop={'size': 10})
                        # plt.xlabel('Time (minutes)')
                        # plt.ylabel('vDisk Hit Ratio')
                        # beg_sec = 4500
                        # end_sec = 9900
                        # if wload == 'rreal.4hr' or wload == 'scr_merg':
                        #     plt.ylim([0.4, 1.0])
                        # else:
                        #     plt.ylim([0.2, 1.0])
                        # plt.xlim([beg_sec, end_sec])
                        # plt.xticks(np.arange(beg_sec + 900, end_sec - 900 + 1, 1800),
                        #            np.arange((beg_sec + 900) / 60, (end_sec - 900) / 60 + 1, 30).astype(int))
                        # plt.savefig(basedir_pre[:-1] + '-vdisk-hr.png', dpi=300, bbox_inches='tight')
                        #############################################################################################
                        # plt.clf()
                        # for vd in range(len(vdisks)):
                        #     plt.plot(x[moment:moment + 5 * 60], hrs_wop_vdisk[vd][moment:moment + 5 * 60],
                        #              color=colors[vd], linewidth=1.2, label='vDisk ' + str(vd + 1), alpha=1)
                        # plt.legend(bbox_to_anchor=(1.03, 1.0), loc='upper left', fancybox=True, prop={'size': 10})
                        # plt.xlabel('Time (minutes)')
                        # plt.ylabel('vDisk Hit Ratio')
                        # beg_sec = moment
                        # end_sec = moment + 5 * 60
                        # if wload == 'rreal.4hr' or wload == 'scr_merg':
                        #     plt.ylim([0.4, 1.0])
                        # else:
                        #     plt.ylim([0.2, 1.0])
                        # plt.xlim([beg_sec, end_sec])
                        # plt.xticks(np.arange(beg_sec, end_sec + 1, 60),
                        #            np.arange(beg_sec / 60, end_sec / 60 + 1, 1).astype(int))
                        # plt.savefig(basedir_pre[:-1] + '-vdisk-hr5-wop.png', dpi=300, bbox_inches='tight')
                        # plt.clf()
                        # for vd in range(len(vdisks)):
                        #     plt.plot(x[moment:moment + 5 * 60], hrs_wp_vdisk[vd][moment:moment + 5 * 60],
                        #              color=colors[vd], linewidth=1.2, label='vDisk ' + str(vd + 1), alpha=1)
                        # plt.legend(bbox_to_anchor=(1.03, 1.0), loc='upper left', fancybox=True, prop={'size': 10})
                        # plt.xlabel('Time (minutes)')
                        # plt.ylabel('vDisk Hit Ratio')
                        # beg_sec = moment
                        # end_sec = moment + 5 * 60
                        # if wload == 'rreal.4hr' or wload == 'scr_merg':
                        #     plt.ylim([0.4, 1.0])
                        # else:
                        #     plt.ylim([0.2, 1.0])
                        # plt.xlim([beg_sec, end_sec])
                        # plt.xticks(np.arange(beg_sec, end_sec + 1, 60),
                        #            np.arange(beg_sec / 60, end_sec / 60 + 1, 1).astype(int))
                        # plt.savefig(basedir_pre[:-1] + '-vdisk-hr5-wp.png', dpi=300, bbox_inches='tight')
                        #############################################################################################
                        # plt.clf()
                        # plt.xlabel('Time (minutes)')
                        # plt.ylabel('Cache Hit Ratio')
                        # plt.plot(x, hr_wof_cache, color='g', linewidth=1.3, label='no failure', zorder=1, alpha=0.75)
                        # plt.plot(x, hr_wop_cache, color='b', linewidth=1.3, label='no prewarming', zorder=2)
                        # plt.plot(x, hr_wp_cache, color='r', linewidth=1.3, label='with prewarming', zorder=5)
                        # ty = [hr_val_before_fail] * len(x)
                        # for n in range(len(x)):
                        #     if n > tt_hr_val_before_fail_wop or n < tt_hr_val_before_fail_wp:
                        #         ty[n] = 0
                        # plt.plot(x, ty, color='darkgrey', linewidth=0.7, linestyle='--')
                        # plt.legend(loc='center right', fancybox=True, prop={'size': 12})
                        # beg_sec = 3600
                        # end_sec = 12600
                        # if wload == 'wload_6gb':
                        #     plt.ylim([0.4, 0.9])
                        # elif wload == 'wload_4gb':
                        #     plt.ylim([0.5, 0.9])
                        # elif wload == 'rreal.4hr':
                        #     plt.ylim([0.6, 1.0])
                        # elif wload == 'scr_merg':
                        #     plt.ylim([0.6, 1.0])
                        # plt.xlim([beg_sec, end_sec])
                        # plt.xticks(np.arange(beg_sec, end_sec + 1, 1800),
                        #            np.arange(beg_sec / 60, end_sec / 60 + 1, 30).astype(int))
                        # # plt.show()
                        # plt.savefig(basedir_pre[:-1] + '-hr.png', dpi=300, bbox_inches='tight')
                        #############################################################################################
                        # for nn in hr_windows:
                        #     plt.clf()
                        #     plt.xlabel('Time (minutes)')
                        #     plt.ylabel('Cache Hit Ratio')
                        #     plt.plot(x[moment:moment + nn * 60], hr_wop_cache[moment:moment + nn * 60], color='b',
                        #              linewidth=1.3, label='no prewarming', zorder=2)
                        #     plt.plot(x[moment:moment + nn * 60], hr_wp_cache[moment:moment + nn * 60], color='r',
                        #              linewidth=1.3, label='with prewarming', zorder=5)
                        #     plt.legend(loc='lower right', fancybox=True, prop={'size': 12})
                        #     beg_sec = moment
                        #     end_sec = moment + nn * 60
                        #     if wload == 'wload_6gb':
                        #         plt.ylim([0.3, 0.9])
                        #     elif wload == 'wload_4gb':
                        #         plt.ylim([0.5, 0.9])
                        #     elif wload == 'rreal.4hr':
                        #         plt.ylim([0.6, 1.0])
                        #     plt.xlim([beg_sec, end_sec])
                        #     plt.xticks(np.arange(beg_sec, end_sec + 1, nn / 5 * 60),
                        #                np.arange(beg_sec / 60, end_sec / 60 + 1, nn / 5).astype(int))
                        #     # plt.show()
                        #     plt.savefig(basedir_pre[:-1] + '-hr' + str(nn) + '.png', dpi=300, bbox_inches='tight')
                        ##############################################################################################
                        # gain_hr_cache = []
                        # for n in range(moment, len(hr_wp_cache)):
                        #     gain_hr_cache.append(hr_wp_cache[n] - hr_wop_cache[n])
                        # plt.clf()
                        # plt.xlabel('Time (minutes)')
                        # plt.ylabel('Difference in Hit Ratio')
                        # plt.plot(x[moment:], gain_hr_cache, color='g', linewidth=1.3)
                        # beg_sec = moment
                        # end_sec = moment + 900
                        # plt.ylim([0, 0.4])
                        # plt.xlim([beg_sec, end_sec])
                        # plt.xticks(np.arange(beg_sec, end_sec + 1, 300),
                        #            np.arange(beg_sec / 60, end_sec / 60 + 1, 5).astype(int))
                        # plt.savefig(basedir_pre[:-1] + '-diff.png', dpi=300, bbox_inches='tight')
                        ##############################################################################################
                        # mrc_window = 900
                        # nx = x[moment: moment + mrc_window]
                        # plt.clf()
                        # if wload == 'rreal.4hr':
                        #     scale = 10
                        # else:
                        #     scale = 1000
                        # plt.xlabel('Time (minutes)')
                        # plt.ylabel('Total Cache Misses (x' + str(scale) + ')')
                        # plt.plot(nx, wop_misses[moment: moment + mrc_window], color='b', linewidth=1.3,
                        #          label='no prewarming', marker='d', markevery=300)
                        # plt.plot(nx, wp_misses[moment: moment + mrc_window], color='r', linewidth=1.3,
                        #          label='with prewarming', marker='d', markevery=300)
                        # plt.legend(loc='upper left', fancybox=True, prop={'size': 12})
                        # beg_sec = moment
                        # end_sec = moment + mrc_window
                        # max_y = wop_misses[moment + mrc_window]
                        # plt.ylim([0, max_y])
                        # plt.yticks(np.arange(0, max_y + 1, max_y / 5),
                        #            np.arange(0, int(max_y / scale) + 1, (max_y / scale) / 5).astype(int))
                        # plt.xlim([beg_sec, end_sec])
                        # plt.xticks(np.arange(beg_sec, end_sec + 1, 300),
                        #            np.arange(beg_sec / 60, end_sec / 60 + 1, 5).astype(int))
                        # # plt.show()
                        # plt.savefig(basedir_pre[:-1] + '-misses.png', dpi=300, bbox_inches='tight')

    ##################################################################################################################
    ######## EXTRAS
    ##################################################################################################################

    # SSR = 100K
    hatchscale = 10

    basedir_nop = 'results/exp2/' + wload + '/' + snaprates[1] + '/'
    mlim_total_prewarmed_vdisk = []
    mlim_5hr_wp_vdisk = []
    for mlim in range(len(mlimits)):
        basedir_pre = basedir_nop + mlimits[mlim] + '-' + prewarmrates[2] + '-' + \
                      heuristics[0] + '-x/'

        total_prewarmed_vdisk = [0] * len(vdisks)
        for n in range(moment, len(x)):
            fname = basedir_pre + str(n) + 's-SHARE.csv'
            if os.path.isfile(fname):
                with open(fname) as prefile:
                    lines = list(csv.reader(prefile))
                for nn in range(len(lines)):
                    total_prewarmed_vdisk[nn] += int(lines[nn][1])
            else:
                break
        total_prewarmed_vdisk = [round(x / 1048576, 2) for x in total_prewarmed_vdisk]
        mlim_total_prewarmed_vdisk.append(total_prewarmed_vdisk)

        window = 5
        avg_5hr_wp_after_fail = []
        for vd in range(len(vdisks)):
            hr_vdisk = []
            with open(basedir_pre + 'VDISK-' + vdisks[vd][0] + '-HR.csv') as prefile:
                lines = list(csv.reader(prefile))
            for line in lines[moment:]:
                hr_vdisk.append(float(line[1]))
            suml = 0
            numl = 0
            for n in range(moment, moment + window * 60):
                suml += hr_vdisk[n]
                numl += 1
            avg_5hr_wp_after_fail.append(round(suml / numl, 2))
        mlim_5hr_wp_vdisk.append(avg_5hr_wp_after_fail)

    print(mlim_total_prewarmed_vdisk)
    print(mlim_5hr_wp_vdisk)
    print()

    for vd in range(len(vdisks)):
        x = []
        y = []
        for mlim in range(len(mlimits)):
            x.append(mlim_total_prewarmed_vdisk[mlim][vd])
            y.append(mlim_5hr_wp_vdisk[mlim][vd])
        print(x)
        print(y)
        print()
        plt.clf()
        plt.plot(x, y, color='r', linewidth='1.2')
        plt.xlabel('Prewarm Set Size (MB)')
        plt.ylabel('5-minute Hit Ratio')
        # plt.show()
        plt.savefig(basedir_nop + 'hrb-vd' + str(vd + 1) + '.png', dpi=300, bbox_inches='tight')
        # exit(0)

    # for ssr in range(len(snaprates)):
    #     for hrstc in range(len(heuristics)):
    #         for ffn in range(len(frerern_func) if hrstc == 2 else 1):
    #             plt_res_prate_time_wp = []
    #             for prate in range(len(prewarmrates)):
    #                 plt_res_prate_time_wp.append([])
    #                 for mlim in range(len(mlimits)):
    #                     basedir_nop = 'results/exp2/' + wload + '/' + snaprates[ssr] + '/'
    #                     basedir_pre = basedir_nop + mlimits[mlim] + '-' + prewarmrates[prate] + '-' + \
    #                                   heuristics[hrstc] + '-' + ((frerern_func[ffn] + '-') if (hrstc == 2) else '') + \
    #                                   'x/'
    #                     hr_wp_cache = []
    #                     tt_hr_val_before_fail_wp = 14400
    #                     with open(basedir_pre + 'ALL-HR.csv') as prefile:
    #                         lines = list(csv.reader(prefile))
    #                     for line in lines:
    #                         hr_wp_cache.append(float(line[1]))
    #
    #                     for n in range(moment, len(hr_wp_cache)):
    #                         if hr_wp_cache[n] >= hr_val_before_fail:
    #                             tt_hr_val_before_fail_wp = n
    #                             break
    #                     plt_res_prate_time_wp[-1].append(tt_hr_val_before_fail_wp - moment)
    #
    #             plt.clf()
    #             fig, ax = plt.subplots()
    #             num_xlabels = len(mlimits)
    #             ind = np.arange(0, num_xlabels * 2, 2)
    #             width = 0.4
    #             plt.grid(b=True, color='grey', linewidth=0.5, alpha=0.5, axis='y', zorder=100)
    #             b1 = plt.bar(ind + width, plt_res_prate_time_wp[0], width, color='tab:purple', label='∞', zorder=10,
    #                          edgecolor='silver', linewidth=0.2)
    #             b2 = plt.bar(ind + 2 * width, plt_res_prate_time_wp[1], width, color='tab:blue', label='500 MBps',
    #                          zorder=10,
    #                          edgecolor='silver', linewidth=0.2)
    #             b3 = plt.bar(ind + 3 * width, plt_res_prate_time_wp[2], width, color='tab:orange', label='100 MBps',
    #                          zorder=10,
    #                          edgecolor='silver', linewidth=0.2)
    #             b4 = plt.bar(ind + 4 * width, plt_res_prate_time_wp[3], width, color='tab:green', label='50 MBps',
    #                          zorder=10,
    #                          edgecolor='silver', linewidth=0.2)
    #             max_y = RUNTIME_SEC - moment
    #             plt.yticks(np.arange(0, max_y, max_y / 5),
    #                        np.arange(0, int(max_y / 60) + 1, (max_y / 60) / 5).astype(int))
    #
    #             plt.ylim([0, tt_hr_val_before_fail_wop - moment + 120])
    #             plt.ylabel('Rel. Time to $HR_F$ (mins)')
    #             plt.xlabel('Memory Limit (% of target cache - ' + target_cache_size + ')')
    #             plt.xticks(ind + 2.5 * width, mlimits)
    #
    #             for b in [b1, b2, b3, b4]:
    #                 for rect in b:
    #                     height = rect.get_height()
    #                     ax.annotate('{}'.format(int(round(height / 60, 0))),
    #                                 xy=(rect.get_x() + rect.get_width() / 2, height),
    #                                 xytext=(0, 3), textcoords="offset points", ha='center', va='bottom', fontsize=7)
    #             plt.axhline(y=(tt_hr_val_before_fail_wop - moment), linewidth=1.5, linestyle='--', color='tab:cyan')
    #
    #             plt.legend(bbox_to_anchor=(1.01, 1.0), loc='best', title='Prewarm Rate', fancybox=True,
    #                        prop={'size': 10})
    #             plt.savefig(basedir_nop + 'all-cross-time-' + str(snaprates[ssr]) + '-' + heuristics[hrstc] + '.png',
    #                         dpi=300, bbox_inches='tight')
    #             plt.close(fig)

    # for prate in range(len(prewarmrates)):
    #     plt_res_inst_hr_wp = []
    #     plt_res_5_hr_wp = []
    #     plt_res_15_hr_wp = []
    #     plt_res_30_hr_wp = []
    #     plt_res_inst_hr_wop = []
    #     plt_res_5_hr_wop = []
    #     plt_res_15_hr_wop = []
    #     plt_res_30_hr_wop = []
    #     for mlim in range(len(mlimits)):
    #         basedir_nop = 'results/exp2/' + wload + '/' + snaprates[ssr] + '/'
    #         basedir_pre = basedir_nop + mlimits[mlim] + '-' + prewarmrates[prate] + '-' + heuristics[1] + '-x/'
    #         hr_wp_cache = []
    #         with open(basedir_pre + 'ALL-HR.csv') as prefile:
    #             lines = list(csv.reader(prefile))
    #         for line in lines:
    #             hr_wp_cache.append(float(line[1]))
    #
    #         avg_hr_wp_after_fail = []
    #         for nn in hr_windows:
    #             suml = 0
    #             numl = 0
    #             for n in range(moment, moment + nn * 60):
    #                 suml += hr_wp_cache[n]
    #                 numl += 1
    #             avg_hr_wp_after_fail.append(round(suml / numl, 2))
    #         inst_hr_wp = hr_wp_cache[moment]
    #         plt_res_inst_hr_wp.append(inst_hr_wp)
    #         plt_res_inst_hr_wop.append(inst_hr_wop)
    #         plt_res_5_hr_wp.append(avg_hr_wp_after_fail[0])
    #         plt_res_5_hr_wop.append(avg_hr_wop_after_fail[0])
    #         plt_res_15_hr_wp.append(avg_hr_wp_after_fail[1])
    #         plt_res_15_hr_wop.append(avg_hr_wop_after_fail[1])
    #         plt_res_30_hr_wp.append(avg_hr_wp_after_fail[2])
    #         plt_res_30_hr_wop.append(avg_hr_wop_after_fail[2])
    #
    #     plt.clf()
    #     fig, ax = plt.subplots()
    #     num_xlabels = len(mlimits)
    #     ind = np.arange(0, num_xlabels * 2, 2)
    #     width = 0.4
    #     plt.grid(b=True, color='grey', linewidth=0.5, alpha=0.5, axis='y', zorder=1)
    #     plt.rcParams.update({'hatch.linewidth': '0.5'})
    #     b11 = plt.bar(ind + width, plt_res_inst_hr_wop, width, color='tab:purple', zorder=10, edgecolor='silver',
    #                   linewidth=0.2, hatch=hatchscale * '/')
    #     b1 = plt.bar(ind + width, plt_res_inst_hr_wp, width, color='tab:purple', label='instantaneous',
    #                  zorder=5, edgecolor='silver', linewidth=0.2)
    #
    #     b22 = plt.bar(ind + 2 * width, plt_res_5_hr_wop, width, color='tab:green', zorder=10, edgecolor='silver',
    #                   linewidth=0.2, hatch=hatchscale * '\\')
    #     b2 = plt.bar(ind + 2 * width, plt_res_5_hr_wp, width, color='tab:green', label='5-minute-avg',
    #                  zorder=5, edgecolor='silver', linewidth=0.2)
    #
    #     b33 = plt.bar(ind + 3 * width, plt_res_15_hr_wop, width, color='tab:orange', zorder=10, edgecolor='silver',
    #                   linewidth=0.2, hatch=hatchscale * '/')
    #     b3 = plt.bar(ind + 3 * width, plt_res_15_hr_wp, width, color='tab:orange', label='15-minute-avg',
    #                  zorder=5, edgecolor='silver', linewidth=0.2)
    #
    #     b44 = plt.bar(ind + 4 * width, plt_res_30_hr_wop, width, color='tab:blue', zorder=10, edgecolor='silver',
    #                   linewidth=0.2, hatch=hatchscale * '\\')
    #     b4 = plt.bar(ind + 4 * width, plt_res_30_hr_wp, width, color='tab:blue', label='30-minute-avg',
    #                  zorder=5, edgecolor='silver', linewidth=0.2)
    #
    #     plt.ylim([0.3, 1.0])
    #     plt.ylabel('Cache Hit Ratio')
    #     plt.xlabel('Memory Limit (% of target cache - ' + target_cache_size + ')')
    #     plt.xticks(ind + 2.5 * width, mlimits)
    #
    #     rects = [[b1, b11], [b2, b22], [b3, b33], [b4, b44]]
    #     for rect in rects:  # , [b3, b33], [b4, b44]]:
    #         for n in range(len(rect[0])):
    #             height = rect[0][n].get_height()
    #             ax.annotate('.{}'.format(int(round(height, 2) * 100)),
    #                         xy=(rect[0][n].get_x() + rect[0][n].get_width() / 2, height),
    #                         xytext=(0, 3), textcoords="offset points", ha='center', va='bottom', fontsize=6)
    #
    #     plt.legend(bbox_to_anchor=(1.01, 1.0), loc='upper left', title='Hit Ratio Variants',
    #                fancybox=True, prop={'size': 10})
    #     plt.savefig(basedir_nop + 'all-cross-hr-' + str(snaprates[ssr]) + '-' + prewarmrates[prate] + '.png',
    #                 dpi=300, bbox_inches='tight')
    #
    # for mlim in range(len(mlimits)):
    #     plt_res_inst_hr_wp = []
    #     plt_res_5_hr_wp = []
    #     plt_res_15_hr_wp = []
    #     plt_res_30_hr_wp = []
    #     plt_res_inst_hr_wop = []
    #     plt_res_5_hr_wop = []
    #     plt_res_15_hr_wop = []
    #     plt_res_30_hr_wop = []
    #     for prate in range(len(prewarmrates)):
    #         basedir_nop = 'results/exp2/' + wload + '/' + snaprates[ssr] + '/'
    #         basedir_pre = basedir_nop + mlimits[mlim] + '-' + prewarmrates[prate] + '-' + heuristics[1] + '-x/'
    #         hr_wp_cache = []
    #         with open(basedir_pre + 'ALL-HR.csv') as prefile:
    #             lines = list(csv.reader(prefile))
    #         for line in lines:
    #             hr_wp_cache.append(float(line[1]))
    #
    #         avg_hr_wp_after_fail = []
    #         for nn in hr_windows:
    #             suml = 0
    #             numl = 0
    #             for n in range(moment, moment + nn * 60):
    #                 suml += hr_wp_cache[n]
    #                 numl += 1
    #             avg_hr_wp_after_fail.append(round(suml / numl, 2))
    #         inst_hr_wp = hr_wp_cache[moment]
    #         plt_res_inst_hr_wp.append(inst_hr_wp)
    #         plt_res_inst_hr_wop.append(inst_hr_wop)
    #         plt_res_5_hr_wp.append(avg_hr_wp_after_fail[0])
    #         plt_res_5_hr_wop.append(avg_hr_wop_after_fail[0])
    #         plt_res_15_hr_wp.append(avg_hr_wp_after_fail[1])
    #         plt_res_15_hr_wop.append(avg_hr_wop_after_fail[1])
    #         plt_res_30_hr_wp.append(avg_hr_wp_after_fail[2])
    #         plt_res_30_hr_wop.append(avg_hr_wop_after_fail[2])
    #
    #     plt.clf()
    #     fig, ax = plt.subplots()
    #     num_xlabels = len(prewarmrates)
    #     ind = np.arange(0, num_xlabels * 2, 2)
    #     width = 0.4
    #     plt.grid(b=True, color='grey', linewidth=0.5, alpha=0.5, axis='y', zorder=1)
    #     plt.rcParams.update({'hatch.linewidth': '0.5'})
    #     b11 = plt.bar(ind + width, plt_res_inst_hr_wop, width, color='tab:purple', zorder=10, edgecolor='darkgrey',
    #                   linewidth=0.2, hatch=hatchscale * '/')
    #     b1 = plt.bar(ind + width, plt_res_inst_hr_wp, width, color='tab:purple', label='instantaneous',
    #                  zorder=5, edgecolor='silver', linewidth=0.2)
    #
    #     b22 = plt.bar(ind + 2 * width, plt_res_5_hr_wop, width, color='tab:green', zorder=10, edgecolor='darkgrey',
    #                   linewidth=0.2, hatch=hatchscale * '\\')
    #     b2 = plt.bar(ind + 2 * width, plt_res_5_hr_wp, width, color='tab:green', label='5-minute-avg',
    #                  zorder=5, edgecolor='silver', linewidth=0.2)
    #
    #     b33 = plt.bar(ind + 3 * width, plt_res_15_hr_wop, width, color='tab:orange', zorder=10,
    #                   edgecolor='darkgrey',
    #                   linewidth=0.2, hatch=hatchscale * '/')
    #     b3 = plt.bar(ind + 3 * width, plt_res_15_hr_wp, width, color='tab:orange', label='15-minute-avg',
    #                  zorder=5, edgecolor='silver', linewidth=0.2)
    #
    #     b44 = plt.bar(ind + 4 * width, plt_res_30_hr_wop, width, color='tab:blue', zorder=10, edgecolor='darkgrey',
    #                   linewidth=0.2, hatch=hatchscale * '\\')
    #     b4 = plt.bar(ind + 4 * width, plt_res_30_hr_wp, width, color='tab:blue', label='30-minute-avg',
    #                  zorder=5, edgecolor='silver', linewidth=0.2)
    #
    #     plt.ylim([0.3, 1.0])
    #     plt.ylabel('Cache Hit Ratio')
    #     plt.xlabel('Prewarm Rate')
    #     plt.xticks(ind + 2.5 * width, pratenames)
    #
    #     rects = [[b1, b11], [b2, b22], [b3, b33], [b4, b44]]
    #     for rect in rects:
    #         for n in range(len(rect[0])):
    #             height = rect[0][n].get_height()
    #             ax.annotate('.{}'.format(int(round(height, 2) * 100)) if height < 1 else '{}'.format(int(height)),
    #                         xy=(rect[0][n].get_x() + rect[0][n].get_width() / 2, height),
    #                         xytext=(0, 3), textcoords="offset points", ha='center',
    #                         va='bottom', fontsize=6)
    #
    #             plt.legend(bbox_to_anchor=(1.01, 1.0), loc='upper left', title='Hit Ratio Variants',
    #                        fancybox=True, prop={'size': 10})
    #             plt.savefig(basedir_nop + 'all-cross-hr-' + str(snaprates[ssr]) + '-' + mlimits[mlim] + '.png',
    #                         dpi=300, bbox_inches='tight')


if __name__ == '__main__':
    # matplotlib.use('agg')
    threads = []
    wloads = [('wload_6gb', '6 GiB'), ('wload_4gb', '4 GiB'), ('rreal.4hr', '32 MiB'), ('scr_merg', '64 MiB')]
    wload = wloads[2]
    # for wload in wloads:
    # x = threading.Thread(target=main, args=(wload[0], wload[1]))
    # threads.append(x)
    # x.start()
    main(wload[0], wload[1])

    # for thread in threads:
    #     thread.join()
