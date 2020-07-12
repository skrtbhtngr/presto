#include "policy.h"

void insert_lru_list(int c, Pair *list[], Pair *val, int ts)
{
    if(list[FRONT])
        assert(list[FRONT]->meta[c]->lru.next);
    if (list[FRONT] == nullptr)
    {
        val->meta[c]->lru.next = val;
        val->meta[c]->lru.prev = val;
        val->meta[c]->lru.ts = ts;
        list[FRONT] = list[REAR] = val;
        return;
    }
    val->meta[c]->lru.next = list[FRONT];
    val->meta[c]->lru.prev = list[REAR];
    val->meta[c]->lru.ts = ts;
    list[REAR]->meta[c]->lru.next = val;
    list[FRONT]->meta[c]->lru.prev = val;
    list[REAR] = val;
}

void delete_lru_list(int c, Pair *list[], Pair *val)
{
    Pair *prev, *next;
    if (list[FRONT] == list[REAR])
    {
        list[FRONT] = list[REAR] = nullptr;
        return;
    }
    if (val == list[FRONT])
        list[FRONT] = list[FRONT]->meta[c]->lru.next;
    else if (val == list[REAR])
        list[REAR] = list[REAR]->meta[c]->lru.prev;
    prev = val->meta[c]->lru.prev;
    next = val->meta[c]->lru.next;
    prev->meta[c]->lru.next = next;
    next->meta[c]->lru.prev = prev;
}

Pair *evict_lru_list(int c, Pair *list[])
{
    Pair *ev = list[FRONT];
    Pair *prev, *next;

    if (list[FRONT] == list[REAR])
    {
        list[FRONT] = list[REAR] = nullptr;
        return ev;
    }
    prev = ev->meta[c]->lru.prev;
    next = ev->meta[c]->lru.next;
    prev->meta[c]->lru.next = next;
    next->meta[c]->lru.prev = prev;
    list[FRONT] = next;
    return ev;
}

//void insert_lfu_list(int c, LFUList **list, Pair *val)
//{
//    LFUList *fnode;
//    if (*list == nullptr || (*list)->freq != 1)
//    {
//        fnode = new LFUList;
//        fnode->freq = 1;
//        fnode->prev = nullptr;
//        fnode->next = *list;
//        if (*list)
//            (*list)->prev = fnode;
//        *list = fnode;
//
//        val->meta[c]->lfu.next = val;
//        val->meta[c]->lfu.prev = val;
//        val->meta[c]->lfu.parent = fnode;
//        fnode->head = val;
//        //fnode->val = 1;
//    }
//    else
//    {
//        val->meta[c]->lfu.prev = (*list)->head->meta[c]->lfu.prev;
//        val->meta[c]->lfu.next = (*list)->head;
//        (*list)->head->meta[c]->lfu.prev->meta[c]->lfu.next = val;
//        (*list)->head->meta[c]->lfu.prev = val;
//        (*list)->head = val;
//        //(*list)->val++;
//        val->meta[c]->lfu.parent = *list;
//    }
//}
//
//void update_lfu_list(int c, LFUList **list, Pair *val)
//{
//    LFUList *node = val->meta[c]->lfu.parent;
//    LFUList *n;
//
//    if (val->meta[c]->lfu.next == val)
//        node->head = nullptr;
//    else
//    {
//        val->meta[c]->lfu.next->meta[c]->lfu.prev = val->meta[c]->lfu.prev;
//        val->meta[c]->lfu.prev->meta[c]->lfu.next = val->meta[c]->lfu.next;
//        if (node->head == val)
//            node->head = val->meta[c]->lfu.next;
//    }
//
//    if (!node->next || node->next->freq != node->freq + 1)
//    {
//        n = new LFUList;
//        n->freq = node->freq + 1;
//        if (node->next)
//            node->next->prev = n;
//        n->next = node->next;
//        n->prev = node;
//        node->next = n;
//        n->head = val;
//        val->meta[c]->lfu.next = val;
//        val->meta[c]->lfu.prev = val;
//        //n->val = 1;
//        val->meta[c]->lfu.parent = n;
//        node = node->next;
//    }
//    else
//    {
//        node = node->next;
//        if (node->head)
//        {
//            val->meta[c]->lfu.prev = node->head->meta[c]->lfu.prev;
//            val->meta[c]->lfu.next = node->head;
//            node->head->meta[c]->lfu.prev->meta[c]->lfu.next = val;
//            node->head->meta[c]->lfu.prev = val;
//        }
//        node->head = val;
//        //node->val++;
//        val->meta[c]->lfu.parent = node;
//    }
//
//    node = node->prev;
//    if (node->head == nullptr)
//    {
//        if (!node->prev)
//        {
//            *list = node->next;
//            node->next->prev = nullptr;
//        }
//        else
//        {
//            node->prev->next = node->next;
//            node->next->prev = node->prev;
//        }
//        free(node);
//    }
//    //else
//    //    node->val--;
//}
//
//void delete_lfu_list(int c, LFUList **list, Pair *val)
//{
//    LFUList *node = val->meta[c]->lfu.parent;
//    if (val->meta[c]->lfu.next == val)
//    {
//        if (*list == node)
//            *list = node->next;
//        if (node->next)
//            node->next->prev = nullptr;
//        free(node);
//    }
//    else
//    {
//        val->meta[c]->lfu.next->meta[c]->lfu.prev = val->meta[c]->lfu.prev;
//        val->meta[c]->lfu.prev->meta[c]->lfu.next = val->meta[c]->lfu.next;
//        if (node->head == val)
//            node->head = val->meta[c]->lfu.next;
//        //node->val--;
//    }
//}
//
//Pair *evict_lfu_list(int c, LFUList **list)
//{
//    Pair *ev;
//    LFUList *node = *list;
//    ev = node->head->meta[c]->lfu.prev;
//    if (node->head->meta[c]->lfu.next == node->head)
//    {
//        if (*list == node)
//            *list = node->next;
//        if (node->next)
//            node->next->prev = nullptr;
//        free(node);
//    }
//    else
//    {
//        ev->meta[c]->lfu.next->meta[c]->lfu.prev = ev->meta[c]->lfu.prev;
//        ev->meta[c]->lfu.prev->meta[c]->lfu.next = ev->meta[c]->lfu.next;
//    }
//    return ev;
//}
