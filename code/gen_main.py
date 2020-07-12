import yaml
from gen import *

conf_template = {
    'name': 'syn501',
    'set': 'wss',
    'vdisk': {'id': 501, 'size': '100gb'},
    'randseed': 1048576,
    'duration': '4h',
    'iops': {'randvar': 'normal', 'mean': 100, 'stddev': 20},
    'sector': {
        'size': '512b',
        'cluster': {
            'size': {'min': 1, 'max': 10},
            'stride': {'min': 1, 'max': 10000}}
    },
    'hotspots': [
        ['50%', '20%'],
        ['10%', '30%'],
        ['40%', '10%']
    ],
    'readp': '50%',
    'iosizes': [
        ['80%', '4096b'],
        ['10%', '8192b'],
        ['5%', '16384b'],
        ['5%', '131072b']
    ]
}


def main():
    iops_list = [
        [25, 5],
        [50, 10],
        [100, 20],
        [150, 30],
        [200, 40],
        [300, 60],
    ]
    cluster_size_list = [
        [1, 10], [1, 20], [1, 30]
    ]
    cluster_stride_list = [
        [1, 10000], [1, 2000], [1, 1000]
    ]
    hotspot_list = [
        [[10, 1], [10, 1], [30, 10], [40, 20], [10, 1]],
        [[90, 80], [5, 1], [5, 1]],
        [[25, 10], [25, 10], [25, 10], [25, 10]],
        [[20, 5], [20, 5], [20, 5], [20, 5], [20, 5]],
        [[20, 80], [80, 20]],
        [[10, 1], [10, 1], [10, 1], [10, 2], [10, 2], [10, 2], [10, 3], [10, 3], [10, 3], [10, 3]],
        [[5, 1], [90, 50], [5, 1]],
        [[20, 1], [20, 2], [20, 3], [20, 4], [20, 5]],
        [[50, 5], [50, 5]],
        [[10, 25], [20, 25], [30, 25], [40, 25]]
    ]
    disk_used = [33, 82, 40, 25, 100, 21, 52, 15, 10, 100]

    conf_template['iops']['mean'] = iops_list[2][0]
    conf_template['iops']['stddev'] = iops_list[2][1]
    conf_template['set'] = 'wss_iops' + str(iops_list[2][0])

    file = open('traces/' + conf_template['set'] + '.csv', 'w')
    sys.stdout = open('traces/' + conf_template['set'] + '-log.txt', 'w')
    if not os.path.exists('traces/' + conf_template['set']):
        os.mkdir('traces/' + conf_template['set'])
    i = 0
    for csize in cluster_size_list:
        for cstride in cluster_stride_list:
            for hs in range(len(hotspot_list)):
                conf_template['name'] = 'syn' + str(501 + i)
                conf_template['vdisk']['id'] = 501 + i
                conf_template['sector']['cluster']['size'] = {'min': csize[0], 'max': csize[1]}
                conf_template['sector']['cluster']['stride'] = {'min': cstride[0], 'max': cstride[1]}
                conf_template['hotspots'] = hotspot_list[hs]
                print(csize[0], csize[1], cstride[0], cstride[1], "HS" + str(hs + 1), disk_used[hs])
                generate(conf_template)
                # file.write("%d,%d,%s/%s.csv\n" % (conf_template['vdisk']['id'],
                #                                   get_size_gb(conf_template['vdisk']['size']),
                #                                   conf_template['set'], conf_template['name']))
                i += 1
                # sys.stdout.write(conf_template['name'] + ' done!')

                # file.close()
            break


def gen_from_file(fname):
    with open(fname) as wl:
        data = yaml.load(wl, Loader=yaml.FullLoader)
        for conf in data:
            print(conf)
            generate(conf)


if __name__ == "__main__":
    main()
