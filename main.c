#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

#define K 1000

struct Data {
    int *first_arr;
    int *second_arr;
    int lenght;
};

struct Result {
    int *zero_carry;     // first value when sent zero carry
    int *positive_carry; // second value when sent non zero carry
    int carry;
};

int Pow(int a, int b) {
    if (b == 0) {
        return 1;
    } else {
        return a << b;
    }
}

struct Result Add(int *first, int *second, int size) {
    struct Result ans;
    ans.zero_carry = calloc(size, sizeof(int));
    ans.positive_carry = calloc(size, sizeof(int));
    ans.carry = 0;

    for (int i = 0; i < size; ++i) {
        ans.zero_carry[i] = (first[i] + second[i] + ans.carry) % 10;
        ans.positive_carry[i] = (first[i] + second[i] + ans.carry) % 10;
        ans.carry = (first[i] + second[i] + ans.carry) / 10;
        
    }
    ans.carry = 0;
    for (int i = 0; i < size; ++i) {
        if (i == 0) {
            ans.positive_carry[i] = (first[i] + second[i] + ans.carry + 1) % 10;
            ans.carry = (first[i] + second[i] + ans.carry + 1) / 10;
        } else {
            ans.positive_carry[i] = (first[i] + second[i] + ans.carry) % 10;
            ans.carry = (first[i] + second[i] + ans.carry) / 10;
        }
    }
    //printf("Carry is %d\n", ans.carry);

    return ans;
}

void GetData(struct Data *data, FILE *file) {

    char digit;
    int size = 0;

    while (fscanf(file, "%c", &digit)) {
        if (digit == '\n')
            break;
        if (!isdigit(digit)) {
            printf("Error: Invalid data format!\n");
            exit(1);
        }
        
        size++;
    }

    data->lenght = size;
    data->first_arr = calloc(size, sizeof(int));
    data->second_arr = calloc(size, sizeof(int));

    fseek(file, 0, SEEK_SET);

    for (int i = size - 1; i >= 0; --i) {
        fscanf(file, "%c", &digit);
        data->first_arr[i] = digit - '0';
    }

    fscanf(file, "\n");

    for (int i = size - 1; i >= 0; --i) {
        fscanf(file, "%c", &digit);
        data->second_arr[i] = digit - '0';
    }
}

int GetLevel(int rank) {
    int k = 0;
    while (rank % 2 == 0) {
        rank /= 2;
        ++k;
    }
    return k;
}

void CheckArgNumb(const int argc) {
    if (argc != 2) {
        printf("Error! Please provide the input file name.\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

void CheckFileExistance(FILE *file) {
    if (file == NULL) {
        printf("Error! Unable to open the file.\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
}

void Upgrade(struct Result *ans, int carry) {
    if (carry == 1) {
        ans->zero_carry = ans->positive_carry;
    }
}


// пробрасывает разряды вниз по группам
void Update(struct Result *ans, int rank, int i, int size) {
    if (((rank % i) < i / 2) || (i == 2)) {
        return;
    }

    if ((rank % i == i / 2) && (i > 2)) {
        MPI_Send(&ans->carry, 1, MPI_INT, rank + i / 4, 0, MPI_COMM_WORLD);
    } else {
        if (rank % i == i / 2 + i / 4) {
            MPI_Recv(&ans->carry, 1, MPI_INT, rank - (i / 4), 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
        int k = GetLevel(rank);
        int is_left = 0;
        if (((rank + Pow(2, k)) % Pow(2, (k + 1))) != 0) {
            is_left = 1;
        }
        if (is_left) {
            MPI_Recv(&ans->carry, 1, MPI_INT, rank + Pow(2, k), 0  , MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        } else {
            MPI_Recv(&ans->carry, 1, MPI_INT, rank - Pow(2, k), 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
        if (k > 0) {
            MPI_Send(&ans->carry, 1, MPI_INT, rank + Pow(2, (k - 1)), 0, MPI_COMM_WORLD);
            MPI_Send(&ans->carry, 1, MPI_INT, rank - Pow(2, (k - 1)), 0, MPI_COMM_WORLD);
        }
    }
}

int main(int argc, char **argv) {

    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    double start_time, end_time;
    int rightest_carry = 0;
    int *result1 = NULL;
    int *result2 = NULL;
    int *result = NULL;



    struct Data data;
    CheckArgNumb(argc);
    if (rank == 0) {
        FILE *file = fopen(argv[1], "r");
        CheckFileExistance(file);
        GetData(&data, file);
    }
    MPI_Bcast(&(data.lenght), 1, MPI_INT, 0, MPI_COMM_WORLD);
    //////////////////////////////////////////////////////////////////////////////////////////////////
    int num_digits_per_proc = data.lenght / size;
    if (data.lenght % size != 0) {
        if (rank == 0) {
            printf("Error! Number of processes is not a divisor of number of digits.\n");
        }
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    //////////////////////////////////////////////////////////////////////////////////////////////////

    int *first_recv = calloc(num_digits_per_proc, sizeof(int));
    int *second_recv = calloc(num_digits_per_proc, sizeof(int));
    int *display = malloc(size * sizeof(int));
    int *send_count = malloc(size * sizeof(int));
    for (int i = 0; i < size; ++i) {
        send_count[i] = num_digits_per_proc;
        display[i] = i * num_digits_per_proc;
    }

    MPI_Scatterv(data.first_arr, send_count, display, MPI_INT, first_recv, num_digits_per_proc, MPI_INT, 0,
                 MPI_COMM_WORLD);
    MPI_Scatterv(data.second_arr, send_count, display, MPI_INT, second_recv, num_digits_per_proc, MPI_INT, 0,
                 MPI_COMM_WORLD);

    start_time = MPI_Wtime();
    for (int j = 0; j < K; ++j) {
        struct Result ans = Add(first_recv, second_recv, num_digits_per_proc);

        result1 = malloc(num_digits_per_proc * size * sizeof(int));
        result2 = malloc(num_digits_per_proc * size * sizeof(int));

        MPI_Gatherv(ans.zero_carry, num_digits_per_proc, MPI_INT, result1, send_count, display, MPI_INT, 0, MPI_COMM_WORLD);
        MPI_Gatherv(ans.positive_carry, num_digits_per_proc, MPI_INT, result2, send_count, display, MPI_INT, 0,
                    MPI_COMM_WORLD);
        MPI_Barrier(MPI_COMM_WORLD);

        if (rank == size - 1) {
            rightest_carry = ans.carry;
            // printf("rightest carry = %d\n", rightest_carry);
        }

        MPI_Bcast(&rightest_carry, 1, MPI_INT, size - 1, MPI_COMM_WORLD); // передача крайнего правого ненулевого разряда 

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        for (size_t i = 2; i <= size; i *= 2) {
            if ((rank - i / 2 + 1) % i == 0) {
                // printf("Sending rank=%d => rank=%d data: %d\n", rank, rank + 1, ans.carry);

                MPI_Send(&ans.carry, 1, MPI_INT, rank + 1, 0, MPI_COMM_WORLD);
            }
            if ((rank - i / 2) % i == 0) {
                int carry = 0;
                MPI_Recv(&carry, 1, MPI_INT, rank - 1, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                // printf("Recieving rank=%d <= rank=%d data: %d\n", rank, rank - 1, carry);
                Upgrade(&ans, carry);
                Update(&ans, rank, i, size);
            }

        }

        result = calloc(num_digits_per_proc * size, sizeof(int));
        MPI_Gatherv(ans.zero_carry, num_digits_per_proc, MPI_INT, result, send_count, display, MPI_INT, 0, MPI_COMM_WORLD);
    }

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    end_time = MPI_Wtime();
    double t = (end_time - start_time) / K;

    if (rank == 0) {
        FILE* output = fopen("output", "w");
        if (rightest_carry != 0) {
            fprintf(output, "%d", rightest_carry);
            printf("%d", rightest_carry);
        }
        for (int i = data.lenght - 1; i >= 0; --i) {
            fprintf(output, "%d", result[i]);
        }
        fclose(output);
        for (int i = data.lenght - 1; i >= 0; --i) {
            printf("%d", result[i]);
        }
        printf("\nAddinig took: %f seconds", t);
    }

    free(first_recv);
    free(second_recv);
    free(result1);
    free(result2);

    MPI_Finalize();

    return 0;
}
