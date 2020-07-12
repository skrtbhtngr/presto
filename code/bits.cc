#include "bits.h"


void BitArray::set_bit(long i)
{
    if (i > bits)
        return;
    unsigned int ele_idx = i / 64;
    unsigned int ele_off = i % 64;
    unsigned long mask = 0x1UL << ele_off;
    bitmap[ele_idx] |= mask;
    onbits++;
}

void BitArray::unset_bit(long i)
{
    if (i > bits)
        return;
    unsigned int ele_idx = i / 64;
    unsigned int ele_off = i % 64;
    unsigned long mask = ~(0x1UL << ele_off);
    bitmap[ele_idx] &= mask;
    onbits--;
}

bool BitArray::isset(long i)
{
    if (i > bits)
        return false;
    unsigned int ele_idx = i / 64;
    unsigned int ele_off = i % 64;
    unsigned long mask = 0x1UL << ele_off;
    return bitmap[ele_idx] & mask;
}

void BitArray::print_bitmap(FILE *fp)
{
    long i, j;
    unsigned long tmp, rem;
    char str[65];
    for (i = 0; i < size; i++)
    {
        tmp = bitmap[i];
        if (!tmp)
        {
            fprintf(fp, "0\n");
            continue;
        }
        memset(str, '0', 65);
        j = 0;
        while (tmp)
        {
            rem = tmp % 2;
            str[63 - j] = rem ? '1' : '0';
            j++;
            tmp /= 2;
        }
        for (j = 0; j < 64; j++)
            fprintf(fp, "%c", str[j]);
        fputc('\n', fp);
    }
    fputc('\n', fp);
}

void BitArray::save_bitmap(FILE *fp)
{
    fwrite(&size, sizeof(unsigned int), 1, fp);
    fwrite(bitmap, sizeof(unsigned long), size, fp);
    fflush(fp);
}


void BitSet::set_bit(long i)
{
    Pair *val;
    unsigned int *k = new unsigned int;
    unsigned int *v;
    unsigned int bit, mask;
    *k = i / 64;
    bit = i % 64;
    val = get(k);
    if (!val)
    {
        v = new unsigned int;
        *v = 0x1UL << bit;
        put(k, v, BITMAP);
    }
    else
    {
        v = (unsigned int *) val->val;
        mask = 0x1UL << bit;
        *v |= mask;
        delete k;
    }
}

void BitSet::unset_bit(long i)
{
    Pair *val;
    unsigned int *k = new unsigned int;
    unsigned int *v;
    unsigned int bit, mask;
    *k = i / 64;
    bit = i % 64;
    val = get(k);
    if (val)
    {
        v = (unsigned int *) val->val;
        mask = ~(0x1UL << bit);
        *v &= mask;
        if (!*v)
        {
            remove(k);
            delete v;
        }
    }
    delete k;
}

void BitArray::copy(BitArray *ba)
{
    assert(size == ba->size);
    onbits = ba->onbits;
    memcpy(bitmap, ba->bitmap, size * sizeof(unsigned long));
}

void BitArray::_or(BitArray *a)
{
    int i;
    for (i = 0; i < a->size; i++)
    {
        this->onbits -= __builtin_popcountll(this->bitmap[i]);
        this->bitmap[i] |= a->bitmap[i];
        this->onbits += __builtin_popcountll(this->bitmap[i]);
    }
}

unsigned int BitArray::_or_bits(BitArray *a, BitArray *b)
{
    int i, onbits = 0;
    for (i = 0; i < a->size; i++)
        onbits += __builtin_popcountll(a->bitmap[i] | b->bitmap[i]);
    return onbits;
}

void BitArray::_and(BitArray *a)
{
    int i;
    for (i = 0; i < a->size; i++)
    {
        this->onbits -= __builtin_popcountll(this->bitmap[i]);
        this->bitmap[i] &= a->bitmap[i];
        this->onbits += __builtin_popcountll(this->bitmap[i]);
    }
    //fprintf(stderr, "onbits: %" PRIu64 "\n", this->onbits);
}

unsigned int BitArray::_and_bits(BitArray *a, BitArray *b)
{
    int i, onbits = 0;
    for (i = 0; i < a->size; i++)
        onbits += __builtin_popcountll(a->bitmap[i] & b->bitmap[i]);
    return onbits;
}

void BitArray::clean()
{
    onbits = 0;
    memset(bitmap, 0, size * sizeof(unsigned long));
}
