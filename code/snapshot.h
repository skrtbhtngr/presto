#include "util.h"
#include "bits.h"

class Snapshot
{
public:
    static int num_epochs;

    int bits;
    int csize;
    unsigned char *bitmap;

    Snapshot(int bits, unsigned long *bitmap, void *tcomp)
    {
        int ret;

        this->bits = bits;
        this->csize = (bits / 8) / 2;
        memset(tcomp, 0, this->csize);

        z_stream zstream;
        zstream.zalloc = Z_NULL;
        zstream.zfree = Z_NULL;
        zstream.opaque = Z_NULL;
        zstream.avail_in = ceil((double) bits / 64) * sizeof(unsigned long);
        zstream.next_in = (Bytef *) bitmap;
        zstream.avail_out = this->csize;
        zstream.next_out = (Bytef *) tcomp;

        deflateInit(&zstream, Z_BEST_COMPRESSION);
        deflate(&zstream, Z_FINISH);
        ret = deflateEnd(&zstream);
        assert(ret == Z_OK);

        this->csize = zstream.total_out;
        this->bitmap = new unsigned char[csize];
        memcpy(this->bitmap, tcomp, this->csize);
        assert(check(bits, bitmap));
    }

    bool check(int bts, unsigned long *bm)
    {
        BitArray *ba = get_bitmap();
        assert(bts == ba->bits);
        int size = ceil((double) bts / 64) * sizeof(unsigned long);
        return memcmp(bm, ba->bitmap, size) == 0;
    }

    BitArray *get_bitmap();

    static void inc_num_snapshots()
    {
        num_epochs++;
    }
};
