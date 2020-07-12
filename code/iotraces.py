import csv

basepath = "/home/skrtbhtngr/CLionProjects/presto/"

TYPE = 0
VDISK = 1
SECTOR = 2
SIZE = 3
TIMESTAMP = 4

RUNTIME_SEC = 14400

SIZE_HM1_OBJ = 1400
SIZE_HM3_OBJ = 27648


def restructure_csv_traces(vdisks):
    for vdisk in vdisks:
        with open(vdisk[2], 'r') as file, open(vdisk[2] + '.tmp', 'w') as tfile:
            lines = csv.reader(file)
            for line in lines:
                vdisk = int(line[1])
                block = int(int(line[2]) / 512)
                size = int(line[3])
                type = str(line[0])
                ts = float(line[4])
                req = "%s,%d,%d,%d,%f\n" % (type, vdisk, block, size, ts)
                tfile.write(req)


def trim(vdisks, time_sec):
    for vdisk in vdisks:
        with open(vdisk[2], 'r') as file, open(vdisk[2] + '.trim', 'w') as tfile:
            lines = csv.reader(file)
            for line in lines:
                type = str(line[0])
                vdisk = int(line[1])
                block = int(line[2])
                size = int(line[3])
                ts = float(line[4])
                if ts >= time_sec:
                    break
                req = "%s,%d,%d,%d,%f\n" % (type, vdisk, block, size, ts)
                tfile.write(req)


def est_size(vdisks):
    for vdisk in vdisks:
        max = 0
        min = 2147483647
        with open(vdisk[2], 'r') as file:
            lines = csv.reader(file)
            for line in lines:
                block = int(line[2])
                if block > max:
                    max = block
                elif block < min:
                    min = block
        print(f"MIN: {(min * 512) / (2 ** 30)} GB, MAX: {(max * 512) / (2 ** 30)} GB")


def main():
    wload = "rreal.4hr"
    vdisks = []

    with open('traces/' + wload + '.csv') as file:
        lines = csv.reader(file)
        for line in lines:
            vdisks.append([int(line[0]), int(line[1]), basepath + 'traces/' + line[2]])

    # trim(vdisks, RUNTIME_SEC)
    est_size(vdisks)


if __name__ == "__main__":
    main()
