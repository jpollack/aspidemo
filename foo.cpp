extern "C" {
#include <aerospike/aerospike.h>
#include <aerospike/aerospike_key.h>
#include <aerospike/aerospike_query.h>
#include <aerospike/as_arraylist.h>
#include <aerospike/as_error.h>
#include <aerospike/as_hashmap.h>
#include <aerospike/as_nil.h>
#include <aerospike/as_query.h>
#include <aerospike/as_exp.h>
#include <aerospike/as_exp_operations.h>
#include <aerospike/as_record.h>
#include <aerospike/as_status.h>
#include <aerospike/as_stringmap.h>
    as_exp *next_in_val_exp (void);
}
#include <unistd.h>
#include <atomic>
#include <algorithm>
#include <cstdlib>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <docopt.h>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <openssl/sha.h>
#include <sstream>
#include <stdexcept>
#include <stdint.h>
#include <string>
#include <sys/time.h>
#include <vector>
#include <signal.h>
#include <random>


inline void __dieunless (const char *msg, const char *file, int line) { fprintf (stderr, "[%s:%d] Assertion '%s' failed.\n", file, line, msg); abort (); }

#define dieunless(EX) ((EX) || (__dieunless (#EX, __FILE__, __LINE__),false))

using namespace std;

static const char USAGE[] =
    R"(usage: foo [options]

options:
  -h --help
  --asdb=SOCKADDR       Aerospike server [default: 127.0.0.1:3000]
  --ns=STRING           Namespace [default: ns0]
  --set=STRING          Set name [default: s0]
  --clear               Reset first

)";

static docopt::Options d;


aerospike as;
atomic<bool> running;
void sigint_handler (int signum) { running = false; }

uint32_t lcg_parkmiller (uint32_t state)
{
    uint64_t p = (uint64_t)state * 48271;
    uint32_t x = (p & 0x7fffffff) + (p >> 31);
    x = (x & 0x7fffffff) + (x >> 31);
    return x;
}
static uint64_t usec_now (void) { return chrono::duration_cast<chrono::microseconds>(chrono::system_clock::now().time_since_epoch()).count(); }

pair<int64_t, int64_t> insertPoint (int64_t ki, double xv, double yv, as_exp *expp)
{
    as_key key0;
    as_key_init_int64 (&key0, "ns0", "s0", ki);

    as_operations ops0;
    as_operations_inita (&ops0, 6);
    
    dieunless (as_operations_add_write_double (&ops0, "x", xv));
    dieunless (as_operations_add_write_double (&ops0, "y", yv));
    dieunless (as_operations_exp_write (&ops0, "in", expp, AS_EXP_WRITE_DEFAULT));
    dieunless (as_operations_add_incr (&ops0, "tot", 1));
    dieunless (as_operations_add_read (&ops0, "in"));
    dieunless (as_operations_add_read (&ops0, "tot"));

    as_error err;
    as_record *recp = NULL;
    dieunless (aerospike_key_operate (&as, &err, nullptr, &key0, &ops0, &recp) == AEROSPIKE_OK);
    dieunless (recp);
    as_bin *results = recp->bins.entries;
    int64_t in = as_bin_get_value (&results[4])->integer.value;
    int64_t tot = as_bin_get_value (&results[5])->integer.value;
    
    as_record_destroy (recp);
    as_operations_destroy (&ops0);
    return {in, tot};
}

int main (int argc, char **argv, char **envp)
{
    d = docopt::docopt (USAGE, {argv+1, argv+argc});
    const string& hostport = d["--asdb"].asString ();
    size_t cpos = hostport.find (':');
    dieunless (cpos != string::npos);
    int portno = stoi (hostport.substr (cpos+1));
    string hostname = hostport.substr (0, cpos);

    as_config config;
    as_config_init (&config);
    dieunless (as_config_add_hosts (&config, hostname.c_str (), portno));

    aerospike_init (&as, &config);

    as_error err;
    dieunless (aerospike_connect (&as, &err) == AEROSPIKE_OK);

    as_key key0;
    int64_t ki = 1000;
    as_key_init_int64 (&key0, "ns0", "s0", ki);

    if (d["--clear"].asBool ()) {
	as_record rec0;
	as_record_inita (&rec0, 4);
	as_record_set_double (&rec0, "x", 0.0f);
	as_record_set_double (&rec0, "y", 0.0f);
	as_record_set_int64 (&rec0, "in", 0);
	as_record_set_int64 (&rec0, "tot", 0);

	dieunless (aerospike_key_put (&as, &err, nullptr, &key0, &rec0) == AEROSPIKE_OK);
    }

    auto expp = next_in_val_exp ();
    uniform_real_distribution<double> rdist0 (0.0, 1.0);
    default_random_engine re;

    signal (SIGINT, sigint_handler);
    int64_t iter{0};
    running = true;
    uint64_t tstart, tlast;
    tstart = tlast = usec_now ();
    while (running) {
	const auto [in, tot] = insertPoint (ki, rdist0 (re), rdist0 (re), expp);
	if (!(++iter % 1024) && (usec_now () >= (tlast + 1000000))) {
	    double piapprox = (double)(4 * in) / (double) tot;
	    tlast = usec_now ();
	    printf ("iteration %d:\t%ld\t%ld\t%lf\n", iter, in, tot, piapprox);
	}
    }

    dieunless (aerospike_close(&as, &err) == AEROSPIKE_OK);
    aerospike_destroy(&as);

    return 0;
}
