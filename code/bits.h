#pragma once

#include "util.h"
#include "hashmap.h"

class BitArray
{
public:
    long bits;
    int size;
    long onbits;
    unsigned long *bitmap;

    BitArray() = default;

    BitArray(long b)
    {
        bits = b;
        onbits = 0;
        size = ceil((double) b / 64);
        bitmap = new unsigned long[size];
        memset(bitmap, 0, size * sizeof(unsigned long));
    }

    BitArray(FILE *fp)
    {
        fread(&size, sizeof(int), 1, fp);
        bitmap = new unsigned long[size];
        fread(bitmap, sizeof(unsigned long), size, fp);
        bits = size * 64;
        onbits = 0;
        for (int i = 0; i < size; i++)
            onbits += __builtin_popcountll(bitmap[i]);
    }

    BitArray(BitArray *ba)
    {
        bits = ba->bits;
        onbits = ba->onbits;
        size = ba->size;
        bitmap = new unsigned long[size];
        memcpy(bitmap, ba->bitmap, size * sizeof(unsigned long));
    }

    ~BitArray()
    {
        delete[] bitmap;
    }

    void _or(BitArray *a);

    static unsigned int _or_bits(BitArray *a, BitArray *b);

    void _and(BitArray *a);

    static unsigned int _and_bits(BitArray *a, BitArray *b);

    void set_bit(long i);

    void unset_bit(long i);

    bool isset(long i);

    void print_bitmap(FILE *fp);

    void save_bitmap(FILE *fp);

    void copy(BitArray *ba);

    void clean();

    void add_snapshot(unsigned char *bm, unsigned int cs, unsigned int epoch);

};


class BitSet : public Hashmap
{
public:
    BitSet(int (*comp)(void *, void *), void (*print)(void *, void *), void (*save)(void *, void *)) : Hashmap(comp,
                                                                                                               print,
                                                                                                               save)
    {}

    void set_bit(long i);

    void unset_bit(long i);
};

//class BloomFilter : public BitArray
//{
//
//    BloomFilter() = default;
//
//    BloomFilter(unsigned long b) : BitArray(b)
//    {}
//
//    void set_bit(unsigned long i);
//
//    void check_bit(unsigned long i);
//
//};
