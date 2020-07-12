#include "util.h"
#include "cache.h"
#include "exp1.h"
#include "exp2.h"
#include "exp3.h"
#include "exp4.h"

int Snapshot::num_epochs;

int experiment = 2;

int main()
{
    fprintf(stderr, "Experiment: %d\n", experiment);
    switch (experiment)
    {
        case 1:
            exp1();
            break;
        case 2:
            exp2();
            break;
        case 3:
            exp3();
            break;
        case 4:
            exp4();
            break;
        default:
            return 1;
    }

    return 0;
}
