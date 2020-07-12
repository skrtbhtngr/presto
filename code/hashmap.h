#pragma once

#include "util.h"
#include "metadata.h"

class Pair;

struct CacheMetaLRU
{
    Pair *prev;
    Pair *next;
    int ts;
};

//struct LFUList
//{
//    unsigned long freq;
//    //unsigned long val;
//    LFUList *next;
//    LFUList *prev;
//    Pair *head;
//};
//
//struct CacheMetaLFU
//{
//    Pair *prev;
//    Pair *next;
//    LFUList *parent;
//};

struct CacheMeta
{
    unsigned char in;
    CacheMetaLRU lru;
    //CacheMetaLFU lfu;
};

enum HMType
{
    HASHMAP1 = 0,
    HASHMAP2 = 1,
    HASHMAP3 = 2,
    BITMAP = 3,
    OTHER = 4
};

class Pair
{
public:
    void *key;
    void *val;
    HMType type;
    CacheMeta *meta[NUM_CACHES];

    Pair()
    {
        key = nullptr;
        val = nullptr;
        for (auto &i : this->meta)
            i = nullptr;
    }

    Pair(void *key, void *value, HMType type)
    {
        this->key = key;
        this->val = value;
        this->type = type;
        for (int i = 0; i < NUM_CACHES; i++)
        {
            this->meta[i] = new CacheMeta;
            memset(this->meta[i], 0, sizeof(CacheMeta));
        }
    }

    inline bool in_cache(int idx)
    { return meta[idx]->in; }

    inline bool in_single(int idx)
    { return meta[idx]->in & IN_SINGLE; }

    inline bool in_multi(int idx)
    { return meta[idx]->in & IN_MULTI; }

    void copy(Pair *p);
};


enum Color
{
    RED = 0,
    BLACK = 1
};

class Node
{
public:
    static Node null;

    Pair data;
    Color color = Color::BLACK;
    Node *parent;
    Node *left;
    Node *right;

    Node()
    {
        this->parent = this;
        this->left = this;
        this->right = this;
    }

    Node(Color c, void *key, void *value, HMType type, Node *p, Node *l, Node *r) : data(key, value, type)
    {
        this->color = c;
        this->parent = p;
        this->left = l;
        this->right = r;
    }

    bool is_left_child()
    { return (this == this->parent->left); }

    bool is_right_child()
    { return (this == this->parent->right); }

    bool isNULL()
    { return this == &Node::null; }

    void copy(Node *n);

};


class Hashmap
{
public:
    Node *root;
    unsigned int num_nodes = 0;
    pthread_rwlock_t lock = PTHREAD_RWLOCK_INITIALIZER;

    Hashmap(int (*comp)(void *, void *), void (*print)(void *, void *), void (*save)(void *, void *))
    {
        this->root = &Node::null;
        this->_comp = comp;
        this->_print = print;
        this->_save = save;
    }

    int put(void *key, void *value, HMType type);

    Pair *get(void *key);

    int remove(void *key);

    int update(void *key, void *value);

    Node *search(void *key);

    void print(FILE *fp);

    void save(FILE *fp);

    void inorder(Node *node, void *data, void (*action)(void *, void *));

    void get_int_keys_rev(Node *node, int *idx, int **arr);

    void copy(Hashmap *hm);

private:
    int (*_comp)(void *a, void *b);

    void (*_print)(void *data, void *fp);

    void (*_save)(void *data, void *fp);


    int insert(Node *n);

    int delet(Node *node);

    Node *minimum(Node *node);

    void transplant(Node *x, Node *y);

    void delete_colorfix(Node *node, Node *tp);

    void rotate_left(Node *x);

    void rotate_right(Node *x);

    void insert_colorfix(Node *n);

};

void print_hm1(void *data, void *fp);

void print_hm3(void *data, void *fp);
