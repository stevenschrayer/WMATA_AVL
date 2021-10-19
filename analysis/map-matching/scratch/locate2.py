# -*- coding: utf-8 -*-
"""
Created on Fri Oct  1 06:48:29 2021

@author: WylieTimmerman
"""

type(y.edges[0][0].get('shape'))


    r_df2 = pd.json_normalize(r.json())
    
        r_df2 = pd.json_normalize(r._content)
    
    r_df2 = (
        r_df
        .explode(
            ['edges'], 
            ignore_index = True
        )
    )
    
    r_df3 = (
        r_df
        .assign(
            shape = lambda x: 
                x
                .edges
                .explode(ignore_index = True)
                .from_dict()
        )
    )
        
        
    r_df3 = (
        r_df
        .assign(
            shape = lambda x: 
                pd.json_normalize(x.edges)
        )
        .assign(
            shape2 = lambda x: pd.json_normalize(shape))
    )
        
        .assign(
            shape = lambda x: pd.explode(['edges'], ignore_index = True)
                # x.edges[0][0].get('shape')
                #afunc(x)
                #x.edges.get('shape')
            # .edge_info.to_dict()[0].get('shape')
        )
    )
    