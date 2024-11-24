#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_LINE_LENGTH 1024
#define MAX_COLUMNS 18

void parse_line(char *line, unsigned char *columns, int *sum1, int *sum2) {
  char *token = strtok(line, ",");
  for (int i = 0; i < MAX_COLUMNS; i++) {
    if (i < 16) {
      columns[i] = (unsigned char)strtol(token, NULL, 10);
    } else if (i == 16) {
      *sum1 = strtol(token, NULL, 10);
    } else {
      *sum2 = strtol(token, NULL, 10);
    }
    token = strtok(NULL, ",");
  }
}

int compare_columns(unsigned char *columns1, unsigned char *columns2) {
  for (int i = 0; i < 16; i++) {
    if (columns1[i] < columns2[i]) {
      return -1;
    } else if (columns1[i] > columns2[i]) {
      return 1;
    }
  }
  return 0;
}

void write_line(FILE *file, unsigned char *columns, int sum1, int sum2) {
  for (int i = 0; i < 16; i++) {
    fprintf(file, "%d,", columns[i]);
  }
  fprintf(file, "%d,%d\n", sum1, sum2);
}

void merge_files(const char *file1, const char *file2, const char *file_out) {
  FILE *f1 = fopen(file1, "r");
  FILE *f2 = fopen(file2, "r");
  FILE *out = fopen(file_out, "w");

  char line1[MAX_LINE_LENGTH];
  char line2[MAX_LINE_LENGTH];

  unsigned char columns1[16];
  unsigned char columns2[16];

  int sum1_1, sum1_2;
  int sum2_1, sum2_2;

  char *res1 = fgets(line1, MAX_LINE_LENGTH, f1);
  char *res2 = fgets(line2, MAX_LINE_LENGTH, f2);

  if (res1)
    parse_line(line1, columns1, &sum1_1, &sum2_1);
  if (res2)
    parse_line(line2, columns2, &sum1_2, &sum2_2);

  while (res1 && res2) {

    int cmp = compare_columns(columns1, columns2);
    if (cmp == 0) {
      write_line(out, columns1, sum1_1 + sum1_2, sum2_1 + sum2_2);
      res1 = fgets(line1, MAX_LINE_LENGTH, f1);
      res2 = fgets(line2, MAX_LINE_LENGTH, f2);
      if (res1)
        parse_line(line1, columns1, &sum1_1, &sum2_1);
      if (res2)
        parse_line(line2, columns2, &sum1_2, &sum2_2);
    } else if (cmp < 0) {
      write_line(out, columns1, sum1_1, sum2_1);
      res1 = fgets(line1, MAX_LINE_LENGTH, f1);
      if (res1)
        parse_line(line1, columns1, &sum1_1, &sum2_1);
    } else {
      write_line(out, columns2, sum1_2, sum2_2);
      res2 = fgets(line2, MAX_LINE_LENGTH, f2);
      if (res2)
        parse_line(line2, columns2, &sum1_2, &sum2_2);
    }
  }

  while (res1) {
    write_line(out, columns1, sum1_1, sum2_1);
    res1 = fgets(line1, MAX_LINE_LENGTH, f1);
    if (res1)
      parse_line(line1, columns1, &sum1_1, &sum2_1);
  }

  while (res2) {
    write_line(out, columns2, sum1_2, sum2_2);
    res2 = fgets(line2, MAX_LINE_LENGTH, f2);
    if (res2)
      parse_line(line2, columns2, &sum1_2, &sum2_2);
  }

  fclose(f1);
  fclose(f2);
  fclose(out);
}

int main(int argc, char *argv[]) {
  if (argc != 4) {
    printf("Usage: ./program file1.csv file2.csv file_out.csv\n");
    return 1;
  }

  const char *file1 = argv[1];
  const char *file2 = argv[2];
  const char *file_out = argv[3];

  merge_files(file1, file2, file_out);

  return 0;
}
