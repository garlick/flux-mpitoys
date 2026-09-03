/************************************************************\
 * Copyright 2026 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, COPYING)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\************************************************************/

/* ovlatency.c - print ping RTT for all TBON edges, sorted by RTT */

#include <stdlib.h>
#include <stdarg.h>
#include <unistd.h>
#include <stdio.h>
#include <mpi.h>
#include <jansson.h>
#include <flux/core.h>

const char *prog = "ovlatency";

void vout (FILE *stream, const char *fmt, va_list ap)
{
    char buf[256];
    vsnprintf (buf, sizeof (buf), fmt, ap);
    fprintf (stream, "%s: %s\n", prog, buf);
}

void diag (const char *fmt, ...)
{
    va_list ap;

    va_start (ap, fmt);
    vout (stderr, fmt, ap);
    va_end (ap);
}

void die (const char *fmt, ...)
{
    va_list ap;

    va_start (ap, fmt);
    vout (stderr, fmt, ap);
    va_end (ap);

    MPI_Abort (MPI_COMM_WORLD, 1);
    //NOTREACHED
}

/* Ping 'dst' rank and record the round trip time, in seconds.
 */
double ping (flux_t *h, int dst)
{
    flux_future_t *f;
    double t_start;
    double t_finish;

    t_start = MPI_Wtime ();
    if (!(f = flux_rpc_pack (h,
                             "broker.ping",
                             dst,
                             0,
                             "{}"))
        || flux_rpc_get (f, NULL) < 0)
        die ("error fetching topology: %s", future_strerror (f, errno));
    t_finish = MPI_Wtime ();
    flux_future_destroy (f);
    return t_finish - t_start;
}

/* Ping 'dst' ping_count times and record [src,dst,min,avg,max] in 'results'.
 */
void ping_add_result (flux_t *h,
                      int src,
                      int dst,
                      int ping_count,
                      json_t *results)
{
    double total_rtt = 0;
    double min_rtt = 0;
    double max_rtt = 0;
    double avg_rtt;
    json_t *entry;

    for (int i = 0; i < ping_count; i++) {
        double rtt = ping (h, dst);
        if (min_rtt == 0 || rtt < min_rtt)
            min_rtt = rtt;
        if (max_rtt == 0 || rtt > max_rtt)
            max_rtt = rtt;
        total_rtt += rtt;
    }
    avg_rtt = total_rtt / ping_count;
    if (!(entry = json_pack ("[iifff]", src, dst, min_rtt, avg_rtt, max_rtt))
        || json_array_append_new (results, entry) < 0)
        die ("could not append to results array");
}

/* Ping each downstream peer.
 * Build a json array representing the results:
 * [[src,dst,...],[src,dst,...],[src,dst,...],...]
 */
json_t *ping_children (flux_t *h, json_t *topo, int ping_count)
{
    json_error_t error;
    int src;          // my rank
    int dst;          // child rank
    json_t *children; // children array (recursive topo)
    size_t index;
    json_t *entry;
    json_t *results;

    if (json_unpack_ex (topo,
                        &error,
                        0,
                        "{s:i s:o}",
                        "rank", &src,
                        "children", &children) < 0)
        die ("error parsing topology: %s", error.text);
    if (!(results = json_array ()))
        die ("could not create json array");
    json_array_foreach (children, index, entry) {
        if (json_unpack_ex (entry, &error, 0, "{s:i}", "rank", &dst) < 0)
            die ("error parsing topology children: %s", error.text);
        ping_add_result (h, src, dst, ping_count, results);
    }
    return results;
}

/* Fetch the TBON topology for the local subtree, then ping children,
 * returning results as a string which the caller must free.
 * Use barriers to ensure each TBON level completes its pings
 * before the next level begins, to avoid interference.
 */
char *ping_children_tostring (flux_t *h, int rank, int ping_count)
{
    json_t *results;
    flux_future_t *f;
    json_t *topo;
    char *s;
    const char *val;
    int level;
    int maxlevel;

    if (!(f = flux_rpc_pack (h,
                             "overlay.topology",
                             FLUX_NODEID_ANY,
                             0,
                             "{s:i}",
                             "rank", rank))
        || flux_rpc_get_unpack (f, "o", &topo) < 0)
        die ("error fetching topology: %s", future_strerror (f, errno));

    if (!(val = flux_attr_get (h, "tbon.level")))
        die ("could not fetch tbon.level attribute");
    level = strtoul (val, NULL, 10);
    if (!(val = flux_attr_get (h, "tbon.maxlevel")))
        die ("could not fetch tbon.maxlevel attribute");
    maxlevel = strtoul (val, NULL, 10);

    /* Make sure all ranks have completed their interaction with the local
     * broker before allowing any ping tests to begin.
     */
    MPI_Barrier (MPI_COMM_WORLD);

    /* Let TBON levels before mine complete.
     */
    for (int i = 0; i < level; i++)
        MPI_Barrier (MPI_COMM_WORLD);

    results = ping_children (h, topo, ping_count);

    /* Let TBON levels mine and after complete.
     */
    for (int i = level; i < maxlevel; i++)
        MPI_Barrier (MPI_COMM_WORLD);

    if (!(s = json_dumps (results, JSON_COMPACT)))
        die ("error encoding results");
    json_decref (results);
    flux_future_destroy (f);
    return s;
}

/* Gather the length of 's' from each rank at rank 0.
 * Then gather 's' from each rank into a buffer and return it and
 * assign an array of string offsets (by rank) to 'offsetsp'.
 * Each string is NULL terminated.
 * See https://stackoverflow.com/questions/31890523
 */
char *gather_strings (int rank,
                      int size,
                      const char *s,
                      int **offsetsp)
{
    int s_size;
    int *s_sizes = NULL;
    int total_size = 0;
    int *offsets = NULL;
    char *buf = NULL;

    s_size = strlen (s) + 1;
    if (rank == 0) {
        if (!(s_sizes = calloc (size, sizeof (int))))
            die ("could not allocate buffer to receive sizes");
    }
    MPI_Gather (&s_size,            // starting address of send buffer
                1,                  // number of elements in send buffer
                MPI_INT,            // datatype of send buffer elements
                s_sizes,            // address of receive buffer
                1,                  // #elements for ay single receive
                MPI_INT,            // datatype of recv buffer elements
                0,                  // rank of receiving process
                MPI_COMM_WORLD);

    if (rank == 0) {
        if (!(offsets = calloc (s_size, sizeof (offsets[0]))))
            die ("could not allocate offsets array");
        for (int i = 0; i < size; i++) {
            total_size += s_sizes[i];
            if (i > 0)
                offsets[i] = offsets[i - 1] + s_sizes[i - 1];
        }
        if (!(buf = calloc (total_size, sizeof (buf[0]))))
            die ("could not allocate buffer to receive data");
    }
    MPI_Gatherv (s,                 // starting address of send buffer
                 s_size,            // number of elements in send buffer
                 MPI_CHAR,          // datatype of send buffer elements
                 buf,               // address of receive buffer
                 s_sizes,           // number of elements from each rank
                 offsets,           // displacement relative to recvbuf
                 MPI_CHAR,          // datatype of recv buffer elements,
                 0,                 // rank of receiving process
                 MPI_COMM_WORLD);

    free (s_sizes);
    *offsetsp = offsets;
    return buf;
}

/* Combine stringified JSON contributions from each rank into one JSON array.
 * Call this on rank 0.
 */
json_t *combine_results (int rank,
                         int size,
                         const char *buf,
                         const int *offsets)
{
    json_t *combined;

    if (!(combined = json_array ()))
        die ("could not allocate results object");
    for (int i = 0; i < size; i++) {
        json_t *o;
        size_t index;
        json_t *entry;

        if (!(o = json_loads (buf + offsets[i], 0, NULL)))
            die ("could not decode results from rank %d", i);
        json_array_foreach (o, index, entry) {
            if (json_array_append (combined, entry) < 0)
                die ("could not append results from rank %d", i);
        }
        json_decref (o);
    }
    return combined;
}

struct qentry {
    int src;
    int dst;
    double min_rtt;
    double avg_rtt;
    double max_rtt;
};

// qsort compare footprint
int qcompare (const void *item1, const void *item2)
{
    const struct qentry *q1 = item1;
    const struct qentry *q2 = item2;
    if (q1->avg_rtt == q2->avg_rtt)
        return 0;
    return q1->avg_rtt < q2->avg_rtt ? -1 : 1;
}

/* Show the combined results array in a useful form,
 * sorted low to high by avg_rtt.  Call this on rank 0.
 */
void display_results (json_t *combined, flux_t *h)
{
    struct qentry *entries;
    int count;
    size_t index;
    json_t *entry;

    count = json_array_size (combined);
    if (!(entries = calloc (count, sizeof (entries[0]))))
        die ("could not allocate final array");

    json_array_foreach (combined, index, entry) {
        if (json_unpack (entry,
                         "[iifff]",
                         &entries[index].src,
                         &entries[index].dst,
                         &entries[index].min_rtt,
                         &entries[index].avg_rtt,
                         &entries[index].max_rtt) < 0)
            die ("could not decode results entry");
    }
    qsort (entries, count, sizeof (entries[0]), qcompare);
    for (int i = 0; i < count; i++) {
        printf ("%s (rank %d) to %s (rank %d):"
                " %.1f msec (min %.1f, max %.1f)\n",
                flux_get_hostbyrank (h, entries[i].src),
                entries[i].src,
                flux_get_hostbyrank (h, entries[i].dst),
                entries[i].dst,
                entries[i].avg_rtt * 1000,
                entries[i].min_rtt * 1000,
                entries[i].max_rtt * 1000);
    }
}

int main (int argc, char **argv)
{
    flux_t *h;
    flux_error_t error;
    int rank;
    int size;
    uint32_t flux_rank;
    char *s;
    char *buf;
    int *offsets;
    int ping_count;

    MPI_Init (&argc, &argv);
    MPI_Comm_rank (MPI_COMM_WORLD, &rank);
    MPI_Comm_size (MPI_COMM_WORLD, &size);

    if (argc != 2 || (ping_count = strtoul (argv[1], NULL, 10)) <= 0)
        die ("Usage: mpirun ovlatency ping_count");

    if (!(h = flux_open_ex (NULL, 0, &error)))
        die ("flux_open: %s", error.text);
    if (flux_get_rank (h, &flux_rank) < 0)
        die ("could not fetch flux rank: %s", strerror (errno));
    if (flux_rank != rank)
        die ("MPI rank %d != flux broker rank %d", rank, (int)flux_rank);

    s = ping_children_tostring (h, rank, ping_count);

    buf = gather_strings (rank, size, s, &offsets);

    if (rank == 0) {
        json_t *combined = combine_results (rank, size, buf, offsets);
        display_results (combined, h);
        json_decref (combined);
    }

    free (offsets);
    free (buf);
    free (s);

    flux_close (h);
    MPI_Finalize ();

    return 0;
}

// vi:ts=4 sw=4 expandtab
