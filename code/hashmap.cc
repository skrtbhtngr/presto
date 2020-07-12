#include "util.h"
#include "hashmap.h"


void print_hm1(void *data, void *fp)
{
    auto *p = (Pair *) data;
    auto *khm = (key_hm1 *) p->key;
    auto *vhm = (val_hm1 *) p->val;
    fprintf((FILE *) fp, "| %s (%-2u, %-7lu) | %6lu |\n", khm->hash,
            khm->eid.vdisk, khm->eid.vblock, vhm->egroup_id);
}

void print_hm3(void *data, void *fp)
{
    auto *p = (Pair *) data;
    auto *khm = (key_hm3 *) p->key;
    auto *vhm = (val_hm3 *) p->val;
    if (vhm->num_extents >= 3)
        fprintf((FILE *) fp, "| %8lu | %2u |\n", khm->egroup_id, vhm->num_extents);
}

//void rbtree_cleanup(struct rbtree *rbt)
//{
//    rbtree_destroy(rbt->root);
//}
//
//void rbtree_&Node::null_cleanup()
//{
//    free(&Node::null);
//}
//
//void rbtree_destroy(struct node *root)
//{
//    if (root != &Node::null)
//    {
//        rbtree_destroy(root->left);
//        rbtree_destroy(root->right);
//        free(root);
//    }
//}

Node Node::null = Node();

Node *Hashmap::search(void *key)
{
    int ret;
    Node *node = root;
    while (!node->isNULL())
    {
        ret = _comp(node->data.key, key);
        if (!ret)
            break;
        node = (ret < 0) ? node->right : node->left;
    }
    return node;
}

void Hashmap::rotate_left(Node *x)
{
    Node *y = x->right;
    x->right = y->left;
    if (!y->left->isNULL())
        y->left->parent = x;
    y->parent = x->parent;
    if (x->parent->isNULL())
        root = y;
    else if (x->is_left_child())
        x->parent->left = y;
    else
        x->parent->right = y;
    y->left = x;
    x->parent = y;
}

void Hashmap::rotate_right(Node *x)
{
    Node *y = x->left;
    x->left = y->right;
    if (!y->right->isNULL())
        y->right->parent = x;
    y->parent = x->parent;
    if (x->parent->isNULL())
        root = y;
    else if (x->is_left_child())
        x->parent->left = y;
    else
        x->parent->right = y;
    y->right = x;
    x->parent = y;
}

void Hashmap::insert_colorfix(Node *n)
{
    Node *uncle, *gparent;
    while (!n->parent->isNULL() && n->parent->color == RED)
    {
        gparent = n->parent->parent;
        if (n->parent->is_left_child())
        {
            uncle = n->parent->parent->right;
            if (!uncle->isNULL() && uncle->color == RED)
            {
                n->parent->color = BLACK;
                gparent->color = RED;
                uncle->color = BLACK;
                n = gparent;
            }
            else
            {
                if (n->is_right_child())
                {
                    n = n->parent;
                    rotate_left(n);
                }
                gparent = n->parent->parent;
                n->parent->color = BLACK;
                gparent->color = RED;
                rotate_right(gparent);
            }
        }
        else
        {
            uncle = n->parent->parent->left;
            if (!uncle->isNULL() && uncle->color == RED)
            {
                n->parent->color = BLACK;
                gparent->color = RED;
                uncle->color = BLACK;
                n = gparent;
            }
            else
            {
                if (n->is_left_child())
                {
                    n = n->parent;
                    rotate_right(n);
                }
                gparent = n->parent->parent;
                n->parent->color = BLACK;
                gparent->color = RED;
                rotate_left(gparent);
            }
        }
    }
    root->color = BLACK;
}

int Hashmap::insert(Node *n)
{
    int ret;
    Node *node = root, *par = &Node::null;
    while (!node->isNULL())
    {
        par = node;
        ret = _comp(node->data.key, n->data.key);
        if (!ret)
            return -1;
        else if (ret < 0)
            node = node->right;
        else
            node = node->left;
    }
    n->parent = par;
    if (par->isNULL())
        root = n;
    else
    {
        ret = _comp(n->data.key, par->data.key);
        if (ret < 0)
            par->left = n;
        else
            par->right = n;
    }
    insert_colorfix(n);
    return 0;
}

int Hashmap::put(void *key, void *value, HMType type)
{
    int rc;
    Node *n = &Node::null;
    try
    {
        n = new Node(RED, key, value, type, &Node::null, &Node::null, &Node::null);
    }
    catch (bad_alloc &ba)
    {
        fprintf(stderr, "Exception in new: %s\n", ba.what());
    }
    pthread_rwlock_wrlock(&lock);
    rc = insert(n);
    if (!rc)
        num_nodes++;
    else
    {
        fprintf(stderr, "Node already inserted!\n");
        delete n;
    }
    pthread_rwlock_unlock(&lock);
    return rc;
}

Pair *Hashmap::get(void *key)
{
    pthread_rwlock_rdlock(&lock);
    Node *node = search(key);
    pthread_rwlock_unlock(&lock);
    if (node->isNULL())
        return nullptr;
    else
        return &node->data;
}

int Hashmap::update(void *key, void *value)
{
    Node *node = search(key);
    if (node->isNULL())
        return -1;
    node->data.val = value;
    return 0;
}

Node *Hashmap::minimum(Node *node)
{
    Node *ret = &Node::null;
    while (!node->isNULL())
    {
        ret = node;
        node = node->left;
    }
    return ret;
}

void Hashmap::transplant(Node *x, Node *y)
{
    if (x->parent->isNULL())
        root = y;
    else if (x->is_left_child())
        x->parent->left = y;
    else
        x->parent->right = y;
    if (!y->isNULL())
        y->parent = x->parent;
}

void Hashmap::delete_colorfix(Node *node, Node *tp)
{
    Node *nnode = &Node::null, *sibling;

    /*
     * this was a very nasty bug!!!
     * if the node to colorfix was `&Node::null`, how do you
     * _get to its parent??
     */
    if (node->isNULL())
    {
        nnode = new Node(BLACK, nullptr, nullptr, node->data.type, tp, &Node::null, &Node::null);
        if (tp->left->isNULL())
            tp->left = nnode;
        else
            tp->right = nnode;
        node = nnode;
    }
    while (node != root && node->color == BLACK)
    {
        if (node->is_left_child())
        {
            sibling = node->parent->right;
            if (sibling->color == RED)
            {
                sibling->color = BLACK;
                node->parent->color = RED;
                rotate_left(node->parent);
                sibling = node->parent->right;
            }
            if ((sibling->left->isNULL() || sibling->left->color == BLACK) &&
                (sibling->right->isNULL() || sibling->right->color == BLACK))
            {
                sibling->color = RED;
                node = node->parent;
            }
            else
            {
                if (sibling->right->color == BLACK)
                {
                    sibling->left->color = BLACK;
                    sibling->color = RED;
                    rotate_right(sibling);
                    sibling = node->parent->right;
                }
                sibling->color = node->parent->color;
                node->parent->color = BLACK;
                sibling->right->color = BLACK;
                rotate_left(node->parent);
                node = root;
            }
        }
        else
        {
            sibling = node->parent->left;
            if (sibling->color == RED)
            {
                sibling->color = BLACK;
                node->parent->color = RED;
                rotate_right(node->parent);
                sibling = node->parent->left;
            }
            if ((sibling->left->isNULL() || sibling->left->color == BLACK) &&
                (sibling->right->isNULL() || sibling->right->color == BLACK))
            {
                sibling->color = RED;
                node = node->parent;
            }
            else
            {
                if (sibling->left->color == BLACK)
                {
                    sibling->right->color = BLACK;
                    sibling->color = RED;
                    rotate_left(sibling);
                    sibling = node->parent->left;
                }
                sibling->color = node->parent->color;
                node->parent->color = BLACK;
                sibling->left->color = BLACK;
                rotate_right(node->parent);
                node = root;
            }
        }
    }
    if (tp->left == nnode)
        tp->left = &Node::null;
    else if (tp->right == nnode)
        tp->right = &Node::null;
    node->color = BLACK;

    if (!nnode->isNULL())
        delete nnode;
}

int Hashmap::delet(Node *node)
{
    Color color = node->color;
    Node *x, *succ;
    Node *tp = node->parent;
    if (node->left->isNULL())
    {
        x = node->right;
        transplant(node, x);
    }
    else if (node->right->isNULL())
    {
        x = node->left;
        transplant(node, x);
    }
    else
    {
        succ = minimum(node->right);
        color = succ->color;
        x = succ->right;
        tp = succ;
        if (succ->parent != node)
        {
            tp = succ->parent;
            transplant(succ, x);
            succ->right = node->right;
            succ->right->parent = succ;
        }

        transplant(node, succ);
        succ->left = node->left;
        succ->left->parent = succ;
        succ->color = node->color;
    }
    if (color == BLACK)
        delete_colorfix(x, tp);

    delete node;
    return 0;
}

int Hashmap::remove(void *key)
{
    int rc;
    pthread_rwlock_rdlock(&lock);
    Node *node = search(key);
    pthread_rwlock_unlock(&lock);
    if (node->isNULL())
        return -1;
    else
    {
        pthread_rwlock_wrlock(&lock);
        rc = delet(node);
        if (!rc)
            num_nodes--;
        pthread_rwlock_unlock(&lock);
        return rc;
    }
}

void Hashmap::inorder(Node *node, void *data, void (*action)(void *, void *))
{
    if (!node->isNULL())
    {
        assert(node->left);
        assert(node->right);
        assert(node->parent);
        inorder(node->left, data, action);
        action((void *) &node->data, data);
        inorder(node->right, data, action);
    }
}

void Hashmap::print(FILE *fp)
{
    pthread_rwlock_rdlock(&lock);
    if (_print)
        inorder(root, fp, _print);
    pthread_rwlock_unlock(&lock);
}

void Hashmap::save(FILE *fp)
{
    pthread_rwlock_rdlock(&lock);
    if (_save)
        inorder(root, fp, _save);
    pthread_rwlock_unlock(&lock);
}

void Hashmap::get_int_keys_rev(Node *node, int *idx, int **arr)
{
    if (!node->isNULL())
    {
        pthread_rwlock_rdlock(&lock);
        get_int_keys_rev(node->right, idx, arr);
        arr[0][*idx] = *((int *) node->data.key);
        arr[1][*idx] = *((int *) node->data.val);
        *idx += 1;
        get_int_keys_rev(node->left, idx, arr);
        pthread_rwlock_unlock(&lock);
    }
}

void Hashmap::copy(Hashmap *hm)
{
    this->num_nodes = hm->num_nodes;
    this->root = new Node;
    this->root->copy(hm->root);
}

void Node::copy(Node *n)
{
    if (!n->isNULL())
    {
        if (!n->left->isNULL())
        {
            this->left = new Node;
            this->left->parent = this;
            this->left->copy(n->left);
        }
        else
            this->left = &Node::null;

        this->color = n->color;
        this->data.copy(&n->data);

        if (!n->right->isNULL())
        {
            this->right = new Node;
            this->right->parent = this;
            this->right->copy(n->right);
        }
        else
            this->right = &Node::null;
    }
}

void Pair::copy(Pair *p)
{
    int i;
    this->type = p->type;
    for (i = 0; i < NUM_CACHES; i++)
    {
        this->meta[i] = new CacheMeta;
        this->meta[i]->in = p->meta[i]->in;
        this->meta[i]->lru.prev = p;
        p->meta[i]->lru.prev = this;
    }
    if (this->type == HASHMAP1)
    {
        this->key = new struct key_hm1;
        this->val = new struct val_hm1;
        memcpy(this->key, p->key, sizeof(struct key_hm1));
        memcpy(this->val, p->val, sizeof(struct val_hm1));
    }
    else
    {
        this->key = new struct key_hm3;
        this->val = new struct val_hm3;
        memcpy(this->key, p->key, sizeof(struct key_hm3));
        memcpy(this->val, p->val, sizeof(struct val_hm3));
    }
}
