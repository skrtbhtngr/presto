import matplotlib.pyplot as plt
import csv
import numpy as np


def main():
    plt.rcParams.update({'font.size': 14})

    heuristics = ['K_FREQ_MEM', 'K_RECN_MEM', 'K_FRERECN_MEM']
    hname = ['k-Frequent', 'k-Recent', 'k-Frerecent']  # constrained
    snaprates = ['10000', '50000', '100000']
    sname = ['10 K', '50 K', '100 K']
    # scores = ['100%', '50%', '33%', '25%', '20%', '1', '2', '3', '4', '5', '1%', '5%', '10%', '15%', '20%']
    # scname = ['100\%', '50\%', '33\%', '25\%', '20\%', '1', '2', '3', '4', '5', '1\%', '5\%', '10\%', '15\%', '20\%']
    mlimits = ['5%', '10%', '25%', '50%', '75%', '100%']
    mname = ['5%', '10%', '25%', '50%', '75%', '100%']
    frerern_func = ['LNR', 'QDR']
    func_name = ['Linear', 'Quadratic']
    prewarmrates = ['0MBps']

    moment = 0

    wload = 'test_wss325'
    wload_file = 'traces/' + wload + '.csv'
    with open(wload_file) as wfile:
        vdisks = list(csv.reader(wfile))

    x = []
    hr_wop_all = []
    basedir_nop = 'results/exp1/' + wload + '/' + snaprates[0] + '/'
    with open(basedir_nop + 'COLD-ALL-HR.txt') as nopfile:
        lines = list(csv.reader(nopfile))
        tt80_wop = 0
        for row in lines:
            x.append(int(row[0]))
            hr_wop_all.append(float(row[1]))
            if float(row[1]) >= 0.8:
                tt80_wop = int(row[0])

    plt.clf()
    plt.xlabel('Time (seconds)')
    plt.ylabel('Hit Ratio')

    vlines = []
    hm1_objs = []
    hr_wop = []
    avg_hr = []
    ttp90 = []
    ttp95 = []
    ttp99 = []
    for vd in range(len(vdisks)):
        with open(basedir_nop + 'COLD-VDISK-' + vdisks[vd][0] + '-HR.csv') as nopfile:
            lines = list(csv.reader(nopfile))
            tt80_wop = 0
            numl = 0
            sum = 0
            flag_ttp90 = False
            flag_ttp95 = False
            flag_ttp99 = False
            for line in lines:
                sum += float(line[1])
                numl += 1
                hr_wop.append(float(line[1]))
                if float(line[1]) >= 0.8:
                    tt80 = int(row[0])

            hr_p99 = hr_wop[-1] * 0.99
            hr_p95 = hr_wop[-1] * 0.95
            hr_p90 = hr_wop[-1] * 0.90
            for line in lines:
                if float(line[1]) >= hr_p90 and not flag_ttp90:
                    ttp90.append(int(line[0]))
                    flag_ttp90 = True
                if float(line[1]) >= hr_p95 and not flag_ttp95:
                    ttp95.append(int(line[0]))
                    flag_ttp95 = True
                if float(line[1]) >= hr_p99 and not flag_ttp99:
                    ttp99.append(int(line[0]))
                    flag_ttp99 = True
            avg = sum / float(numl)

        avg_hr.append(avg)
        plt.plot(x, hr_wop)
        plt.legend(['VDISK' + vdisks[vd][0]])
        vlines.append(hr_wop)

        with open(basedir_nop + 'COLD-VDISK-' + vdisks[vd][0] + '-OBJ.csv') as nopfile:
            lines = list(csv.reader(nopfile))
            hm1_objs.append(int(lines[-1][2]))

    # plt.show()
    print('Avg HR')
    for i in avg_hr:
        print("%.3lf" % i)
    print('HM1 Objs')
    for i in hm1_objs:
        print("%d" % i)
    print('TTP95')
    for i in ttp95:
        print("%d" % i)

    for ssr in range(len(snaprates)):
        for mlim in range(len(mlimits)):
            for prate in range(len(prewarmrates)):
                for hrstc in range(len(heuristics)):
                    for ffn in range(len(frerern_func) if hrstc == 2 else 1):

                        basedir_pre = basedir_nop + mlimits[mlim] + '-' + prewarmrates[prate] + '-' + \
                                      heuristics[hrstc] + '-' + ((frerern_func[ffn] + '-') if (hrstc == 2) else '') + \
                                      '-x/'
                        objs = []
                        ttp95 = []
                        with open(basedir_pre + 'PRE-' + str(moment) + '-SHARE.csv') as prefile:
                            lines = list(csv.reader(prefile))
                        for line in lines:
                            objs.append(int(line[2]))

                        with open(basedir_pre + 'PRE-ALL-HR.txt') as prefile:
                            lines = list(csv.reader(prefile))
                        hr_wp = []
                        tt80_wp = 0
                        for row in lines:
                            hr_wp.append(float(row[1]))
                        if float(row[1]) >= 0.8:
                            tt80_wp = int(row[0])

                        for vd in range(len(vdisks)):
                            hr_p95 = vlines[vd][-1] * 0.95
                            with open(basedir_pre + 'PRE-VDISK-' + vdisks[vd][0] + '-HR.csv') as prefile:
                                lines = list(csv.reader(prefile))
                        sum = 0
                        numl = 0
                        flag_ttp90 = False
                        for line in lines:
                            sum += float(line[1])
                        numl += 1
                        if float(line[1]) >= hr_p95 and not flag_ttp90:
                            ttp95.append(numl - 1)
                        flag_ttp90 = True
                        avg = sum / float(numl)

                        print("%s," % (sname[ssr]), end='')
                        print("%s," % (mname[mlim]), end='')
                        print("%s," % (hname[hrstc]), end='')
                        print("VDISK-%d,%.3lf,%d,%.3lf,%d" % (int(vdisks[vd][0]), avg, objs[vd], hr_p95, ttp95[vd]))

                        '''                        if (len(x)) == 0:
                                                    pss = '-'
                                                    hm1 = hm3 = m12_90_wp = m12_95_wp = m12_99_wp = '-'
                                                    fname = 'results/new/exp1/' + heuristics[hrstc] + '-' + snaprates[ssr] + '-' + \
                                                            (scores[l + 5] if hrstc == 2 else
                                                             (scores[l + 10] if hrstc == 4 else
                                                              (scores[l] if (hrstc % 2) == 0 else 'x'))) + (
                                                                '' if hrstc < 4 else '-' + frerern_func[ffn]) + '-' + \
                                                            mlimits[mlim] + '.txt'
                                                    with open(fname) as file:
                                                        lines = file.readlines()
                                                        for line in lines:
                                                            if "Total prewarm set size after analysis:" in line:
                                                                pss = str.strip(str.split(line, ':')[1])
                        
                                                    fname = 'results/new/exp1/py-metrics.txt'
                                                    with open(fname, 'a') as file:
                                                        line = ' & ' + sname[ssr] + ' & ' + (scname[l + 5] if hrstc == 2 else
                                                                                             (scname[l + 10] if hrstc == 4 else
                                                                                              (scname[l] if (hrstc % 2) == 0 else ''))) + \
                                                               ('' if hrstc < 4 else ' & ' + frerern_func[ffn]) + ' & ' + \
                                                               mname[mlim] + ' & ' + str(pss) + ' & ' + str(hm1) + ' & ' + str(hm3) + \
                                                               ' & ' + str(m12_90_wp) + ' & ' + str(m12_95_wp) + ' & ' + str(
                                                            m12_99_wp) + ' \\\\ \\cline{2-8} \n'
                                                        file.writelines(line)
                                                    continue
                        
                                                hr_90 = 0.90 * wop[-1]
                                                hr_95 = 0.95 * wop[-1]
                                                hr_99 = 0.99 * wop[-1]
                        
                                                m11_90 = round(hr_90, 3)
                                                m11_95 = round(hr_95, 3)
                                                m11_99 = round(hr_99, 3)
                        
                                                for n in range(len(wop)):
                                                    if wop[n] >= hr_99:
                                                        break
                                                m12_99_wop = n
                                                for n in range(len(wp)):
                                                    if wp[n] >= hr_99:
                                                        break
                                                m12_99_wp = n
                        
                                                for n in range(len(wop)):
                                                    if wop[n] >= hr_95:
                                                        break
                                                m12_95_wop = n
                                                for n in range(len(wp)):
                                                    if wp[n] >= hr_95:
                                                        break
                                                m12_95_wp = n
                        
                                                for n in range(len(wop)):
                                                    if wop[n] >= hr_90:
                                                        break
                                                m12_90_wop = n
                                                for n in range(len(wp)):
                                                    if wp[n] >= hr_90:
                                                        break
                                                m12_90_wp = n
                        
                                                print(m11_99)
                                                print(m12_99_wop)
                                                # print(m12_99_wp)
                                                print(m11_95)
                                                print(m12_95_wop)
                                                # print(m12_95_wp)
                                                print(m11_90)
                                                print(m12_90_wop)
                                                # print(m12_90_wp)
                                                print()
                        
                                                break
                        
                                                # print('\nWhere it took 2 hours to get a hit ratio of ' + str(m11) +
                                                #       ' without prewarming,\nprewarming the cache resulted in taking only ' + str(m12) +
                                                #       ' minutes to reach that hit ratio!\n')
                        
                                                avg = 0
                                                for n in range(10):
                                                    avg += diff[n]
                                                avg /= 10
                                                m2 = round(avg, 3)
                        
                                                # print('\nIn the first 10 minutes of storage I/O traffic, the averaged hit ratio increased by '
                                                #       + str(m2) +
                                                #       '\nwhen the cache was prewarmed!\n')
                        
                                                plt.plot(x, wop, color='b', linewidth=1.3, label='no prewarming')
                                                plt.plot(x, wp, color='r', linewidth=1.3, label='with prewarming')
                                                ty = [hr_99] * len(x)
                                                for n in range(len(x)):
                                                    if n < m12_99_wp or n > m12_99_wop:
                                                        ty[n] = 0
                                                plt.plot(x, ty, color='darkgrey', linewidth=0.7, linestyle='--')
                                                # plt.axhline(y=hr, xmin=(n - 330) / (545 - 330 + 1), xmax=1, color='mlim',
                                                #             linewidth=0.5, linestyle='--')
                                                plt.legend(loc='center right', prop={'size': 12})
                                                plt.xlabel('Time (minutes)')
                                                plt.ylabel('Hit ratio')
                                                plt.xlim([-5, 300])
                                                plt.xticks(np.arange(0, 301, 30))
                                                plt.ylim([0.60, 0.90])
                                                plt.savefig(fname + '-comp.png', dpi=300, bbox_inches='tight')
                                                plt.clf()
                        
                                                plt.plot(x, diff, color='g', linewidth=1.3)
                                                plt.xlabel('Time (minutes)')
                                                plt.ylabel('Gain in hit ratio')
                                                plt.xlim([-5, 300])
                                                plt.xticks(np.arange(0, 301, 30))
                                                plt.ylim([0.0, 0.25])
                                                plt.savefig(fname + '-diff.png', dpi=300, bbox_inches='tight')
                                                plt.clf()
                        
                                                pss = '-'
                                                hm1 = '-'
                                                hm3 = '-'
                                                fname = 'results/new/exp1/' + heuristics[hrstc] + '-' + snaprates[ssr] + '-' + \
                                                        (scores[l + 5] if hrstc == 2 else
                                                         (scores[l + 10] if hrstc == 4 else
                                                          (scores[l] if (hrstc % 2) == 0 else 'x'))) + (
                                                            '' if hrstc < 4 else '-' + frerern_func[ffn]) + '-' + \
                                                        mlimits[mlim] + '.txt'
                                                with open(fname) as file:
                                                    lines = file.readlines()
                                                    for line in lines:
                                                        if "Total prewarm set size after analysis:" in line:
                                                            pss = str.strip(str.split(line, ':')[1])
                                                        elif "Needed to fetch" in line:
                                                            hm1 = str.split(line, ' ')[9][1:]
                                                            hm3 = str.split(line, ' ')[12]
                        
                                                fname = 'results/new/exp1/py-metrics.txt'
                                                with open(fname, 'a') as file:
                                                    line = ' & ' + sname[ssr] + ' & ' + (scname[l + 5] if hrstc == 2 else
                                                                                         (scname[l + 10] if hrstc == 4 else
                                                                                          (scname[l] if (hrstc % 2) == 0 else ''))) + \
                                                           ('' if hrstc < 4 else ' & ' + frerern_func[ffn]) + ' & ' + \
                                                           mname[mlim] + ' & ' + str(pss) + ' & ' + str(hm1) + ' & ' + str(hm3) + \
                                                           ' & ' + str(m12_90_wp) + ' & ' + str(m12_95_wp) + ' & ' + str(
                                                        m12_99_wp) + ' \\\\ \\cline{2-8} \n'
                                                    file.writelines(line)
                        
                                            if hrstc % 2:
                                                break
                        
                                        # if hrstc == 0:
                                        #     fname = 'results/new/' + snaprates[ssr] + '.diff'
                                        #     with open(fname) as file:
                                        #         lines = csv.reader(file)
                                        #         x = []
                                        #         for row in lines:
                                        #             x.append(int(row[1]))
                                        #         avg = 0
                                        #         for n in x:
                                        #             avg += n
                                        #         avg /= len(x)
                                        #         print(
                                        #             'Average delta in set bits (between successive snapshots) when using a snapshot rate of ' +
                                        #             snaprates[ssr] + ' is ' + str(int(avg)) + '\n')
                        
                        '''
                        if __name__ == '__main__':
                            main()
