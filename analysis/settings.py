# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 17:58:13 2024

@author: earlm
"""

def get_config(fileio=None):
    import configparser
    from pathlib import Path    
    
    p = Path(__file__).with_name("analysis.cfg")
    
    cfg = configparser.ConfigParser()
    cfg.read(p)
    
    config = {s:dict(cfg.items(s)) for s in cfg.sections()}
    
    return config
   