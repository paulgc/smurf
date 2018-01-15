
from libcpp.vector cimport vector 
from libcpp.string cimport string 
from libcpp.map cimport map as omap

from smurf.utils.extract_rules import extract_pos_rules_from_rf                 
from smurf.utils.generic_helper import convert_dataframe_to_array, \
    get_attrs_to_project, get_num_processes_to_launch, remove_redundant_attrs, \
    find_output_attribute_indices, get_output_header_from_tables 
from smurf.utils.cython_utils cimport compfnptr,\
    get_comparison_function, get_comp_type, int_max, int_min, tokenize_lists  

def execute_rf(ltable, rtable, 
               l_key_attr, r_key_attr, 
               l_join_attr, r_join_attr,
               rf, feature_table,
               allow_empty=True, allow_missing=False,
               l_out_attrs=None, r_out_attrs=None,
               l_out_prefix='l_', r_out_prefix='r_'):

    # remove redundant attrs from output attrs.                                 
    l_out_attrs = remove_redundant_attrs(l_out_attrs, l_key_attr)               
    r_out_attrs = remove_redundant_attrs(r_out_attrs, r_key_attr)               
                                                                                
    # get attributes to project.                                                
    l_proj_attrs = get_attrs_to_project(l_out_attrs, l_key_attr, l_join_attr)   
    r_proj_attrs = get_attrs_to_project(r_out_attrs, r_key_attr, r_join_attr)   

    rule_sets = extract_pos_rules_from_rf(rf, feature_table)
    num_trees = len(rule_sets)

    blocking_set, matching_set = get_trees_for_blocking(rule_sets, feature_table)

    # Do a projection on the input dataframes to keep only the required 
    # attributes. Then, remove rows with missing value in join attribute from 
    # the input dataframes. Then, convert the resulting dataframes into ndarray.
    ltable_array = convert_dataframe_to_array(ltable, l_proj_attrs, l_join_attr)
    rtable_array = convert_dataframe_to_array(rtable, r_proj_attrs, r_join_attr)

    # computes the actual number of jobs to launch.
    n_jobs = min(get_num_processes_to_launch(n_jobs), len(rtable_array))

    if n_jobs <= 1:                                                             
        # if n_jobs is 1, do not use any parallel code.                         
        output_table = execute_rf_split(ltable_array, rtable_array,                 
                                        l_proj_attrs, r_proj_attrs,                 
                                        l_key_attr, r_key_attr,                     
                                        l_join_attr, r_join_attr,                   
                                        allow_empty,            
                                        l_out_attrs, r_out_attrs,                   
                                        l_out_prefix, r_out_prefix,                 
                                        out_sim_score, show_progress)               
    else:                                                                       
        # if n_jobs is above 1, split the right table into n_jobs splits and    
        # join each right table split with the whole of left table in a separate
        # process.                                                              
        r_splits = split_table(rtable_array, n_jobs)                            
        results = Parallel(n_jobs=n_jobs)(delayed(execute_rf_split)(                
                                          ltable_array, r_splits[job_index],    
                                          l_proj_attrs, r_proj_attrs,           
                                          l_key_attr, r_key_attr,               
                                          l_join_attr, r_join_attr,             
                                          allow_empty,      
                                          l_out_attrs, r_out_attrs,             
                                          l_out_prefix, r_out_prefix,           
                                          out_sim_score,                        
                                      (show_progress and (job_index==n_jobs-1)))
                                          for job_index in range(n_jobs))       
        output_table = pd.concat(results) 

    # If allow_missing flag is set, then compute all pairs with missing value in
    # at least one of the join attributes and then add it to the output 
    # obtained from the join.
    if allow_missing:
        missing_pairs = get_pairs_with_missing_value(
                                        ltable, rtable,
                                        l_key_attr, r_key_attr,
                                        l_join_attr, r_join_attr,
                                        l_out_attrs, r_out_attrs,
                                        l_out_prefix, r_out_prefix,
                                        out_sim_score, show_progress) 
        output_table = pd.concat([output_table, missing_pairs])

    # add an id column named '_id' to the output table.
    output_table.insert(0, '_id', range(0, len(output_table)))

    return output_table

def execute_rf_split(ltable, rtable,                   
                     l_columns, r_columns,                       
                     l_key_attr, r_key_attr,                     
                     l_join_attr, r_join_attr,                   
                     allow_empty,                                
                     l_out_attrs, r_out_attrs,                   
                     l_out_prefix, r_out_prefix,                 
                     out_sim_score, show_progress):

    # find column indices of key attr and output attrs in ltable                
    l_key_attr_index = l_columns.index(l_key_attr)                              
    l_join_attr_index = l_columns.index(l_join_attr)                            
    l_out_attrs_indices = find_output_attribute_indices(l_columns, l_out_attrs) 
                                                                                
    # find column indices of key attr and output attrs in rtable                
    r_key_attr_index = r_columns.index(r_key_attr)                              
    r_join_attr_index = r_columns.index(r_join_attr)                            
    r_out_attrs_indices = find_output_attribute_indices(r_columns, r_out_attrs) 

    cdef omap[string, vector[vector[int]]] ltokens_cache, rtokens_cache 
    tokenize_strings(ltable, rtable, l_join_attr_index, r_join_attr_index,
                     ltokens_cache, rtokens_cache)

    

def execute_rules_for_blocking():

cdef void tokenize_strings(ltable, rtable, l_join_attr_index, r_join_attr_index,
                           omap[string, vector[vector[int]]]& ltokens_cache, 
                           omap[string, vector[vector[int]]]& rtokens_cache):
    seen_tok_types = set()
    for rule_set in rule_sets:                                                  
        for rule in rule_set.rules:                                             
            for predicate in rule.predicates:                                   
                if predicate.sim_measure_type == 'EDIT_DISTANCE':               
                    tok_type = 'qg2_bag'
                    tokenizer = QgramTokenizer(qval=2)                              
                else:                                                           
                    tok_type = predicate.tokenizer_type
                    tokenizer = predicate.tokenizer

                if tok_type not in seen_tok_types:
                    ltokens_cache[tok_type] = vector[vector[int]]()                            
                    rtokens_cache[tok_type] = vector[vector[int]]() 
                    tokenize_lists(ltable, rtable, 
                                   l_join_attr_index, r_join_attr_index, 
                                   tokenizer, ltokens_cache[tok_type],
                                   rtokens_cache[tok_type])
                    seen_tok_types.add(tok_type)

