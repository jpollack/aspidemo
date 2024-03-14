#include <aerospike/as_exp.h>

as_exp *next_in_val_exp (void)
{
    as_exp_build (expr,
		  as_exp_cond (
		      as_exp_cmp_le (
			  as_exp_add (
			      as_exp_mul (
				  as_exp_bin_float ("x"), 
				  as_exp_bin_float ("x")),
			      as_exp_mul (
				  as_exp_bin_float ("y"), 
				  as_exp_bin_float ("y"))),
			  as_exp_float (1.0)),
		      as_exp_add (
			  as_exp_bin_int ("in"),
			  as_exp_int (1)),
		      as_exp_bin_int ("in")));
    
    return expr;
}

as_exp *pi_est_exp (void)
{
    as_exp_build (expr,
		  as_exp_div (
		      as_exp_to_float (
		  	  as_exp_mul (
			      as_exp_int (4),
			      as_exp_bin_int ("in"))),
		      as_exp_to_float (
			  as_exp_bin_int ("tot"))));
    return expr;
}
