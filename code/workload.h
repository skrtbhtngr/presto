#include "util.h"

struct vdisk
{
    int vdisk_id;
    int size_gb;
    char *trace_file;
};

class Workload
{
public:
    int num_vdisks;
    struct vdisk *vdisks;
    char *tracefile;
    const char *name;

    Workload(const char *wname)
    {
        int count = 0;
        char *fname;
        char buf[MAX_REQ_LENGTH], *word, *saveptr;
        asprintf(&fname, "%s%s.csv", PRESTO_BASEPATH "traces/", wname);
        FILE *fp = fopen(fname, "r");
        assert(fp != nullptr);
        while (fgets(buf, MAX_REQ_LENGTH, fp))
            count++;
        fseek(fp, 0, SEEK_SET);
        this->name = wname;
        this->num_vdisks = count;
        this->vdisks = new struct vdisk[count];
        count = 0;
        while (fgets(buf, MAX_REQ_LENGTH, fp))
        {
            word = strtok_r(buf, ",\n", &saveptr);
            this->vdisks[count].vdisk_id = strtol(word, nullptr, 0);
            word = strtok_r(nullptr, ",\n", &saveptr);
            this->vdisks[count].size_gb = strtol(word, nullptr, 0);
            word = strtok_r(nullptr, ",\n", &saveptr);
            asprintf(&this->vdisks[count].trace_file, "%s%s", PRESTO_BASEPATH "traces/", word);
            count++;
        }
        fclose(fp);
        asprintf(&tracefile, "%s%s/%sMerged.csv", PRESTO_BASEPATH"traces/", name, name);
    }

    Workload(const char *name, struct vdisk vdisks[], int num_vdisks)
    {
        this->num_vdisks = num_vdisks;
        this->vdisks = vdisks;
        this->name = name;
    }
};