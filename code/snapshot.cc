#include "snapshot.h"

BitArray *Snapshot::get_bitmap()
{
    BitArray *ba = new BitArray(bits);

    z_stream zstream;
    zstream.zalloc = Z_NULL;
    zstream.zfree = Z_NULL;
    zstream.opaque = Z_NULL;
    zstream.avail_in = csize;
    zstream.next_in = (Bytef *) bitmap;
    zstream.avail_out = ceil((double) bits / 64) * sizeof(unsigned long);
    zstream.next_out = (Bytef *) ba->bitmap;

    inflateInit(&zstream);
    inflate(&zstream, Z_NO_FLUSH);
    inflateEnd(&zstream);

    for (int i = 0; i < ba->size; i++)
        ba->onbits += __builtin_popcountll(ba->bitmap[i]);

    return ba;
}
