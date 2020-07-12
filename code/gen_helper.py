import re

RE_SIZE = "^([0-9]+)([KkMmGgTt]?[Bb])$"
RE_SIZE_GB = "^([0-9]+)([Gg][Bb])$"
RE_TIME = "^([0-9]+)([HhMmSs])$"
RE_PERC = "^([0-9]+)(%?)$"


def validate(conf):
    assert re.match(RE_SIZE, conf['vdisk']['size']), "Error: vDisk size not is proper format!"
    assert re.match(RE_TIME, conf['duration']), "Error: duration not is proper format!"
    if 'size' in conf['sector'].keys():
        assert re.match(RE_SIZE, conf['sector']['size']), "Error: sector size not is proper format!"
    assert re.match(RE_PERC, conf['readp']), "Error: read percentage not is proper format!"


def get_size_gb(token):
    assert re.match(RE_SIZE_GB, token), "Error: vDisk size not is proper format!"
    toks = re.match(RE_SIZE_GB, token).groups()
    return int(toks[0])


def get_size_bytes(token):
    toks = re.match(RE_SIZE, token).groups()
    if toks[1].lower() == 'tb':
        return int(toks[0]) * 2 ** 40
    elif toks[1].lower() == 'gb':
        return int(toks[0]) * 2 ** 30
    elif toks[1].lower() == 'mb':
        return int(toks[0]) * 2 ** 20
    elif toks[1].lower() == 'kb':
        return int(toks[0]) * 2 ** 10
    else:
        return int(toks[0])


def get_time_seconds(token):
    toks = re.match(RE_TIME, token).groups()
    if toks[1].lower() == 'h':
        return int(toks[0]) * 3600
    elif toks[1].lower() == 'm':
        return int(toks[0]) * 60
    else:
        return int(toks[0])


def get_perc(token):
    if type(token) == int:
        return token
    else:
        return int(re.match(RE_PERC, token).groups()[0])
