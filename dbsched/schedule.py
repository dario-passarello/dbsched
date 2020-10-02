import collections
import itertools
from collections import defaultdict, namedtuple, Counter
import re
import networkx as nx

Operation = namedtuple('Operation', ['type', 'transaction', 'resource'])
ReadsFrom = namedtuple('ReadsFrom', ['write','read'])

class Schedule:
    def __init__(self, op_list=None, sched_str=None, cache=False):
        self.cache = cache
        self._transaction_dict = None
        self._resource_dict = None
        self.sched = []
        if sched_str is not None:
            self.sched = read_schedule(sched_str)
        elif op_list is not None:
            self.sched = list(op_list)
    
    def __str__(self):
        tot = ""
        for op in self.sched:
            tot += f'{op.type}{op.transaction}({op.resource}) '
        return tot

        
    def transactions(self) -> dict:
        if self.cache and self._transaction_dict is not None:
            return self._transaction_dict
        res_dict = defaultdict(list)
        for op in self.sched:
            res_dict[op.transaction].append(op)
        if self.cache:
            self._transaction_dict = res_dict
        return res_dict
    
    def resources_table(self) -> dict:
        if self.cache and self._resource_dict is not None:
            return self._resource_dict
        res_dict = defaultdict(list)
        for op in self.sched:
            res_dict[op.resource].append(op)
        if self.cache:
            self._resource_dict = res_dict
        return res_dict
    
    def resources_set(self) -> set:
        return {op.resource for op in self.sched}

    def reads_from(self) -> dict:
        res_dict = self.resources_table()
        reads_from = defaultdict(list)
        for res, res_ops in res_dict.items():
            last_write = None
            for op in res_ops:
                if op.type == 'w':
                    last_write = op
                elif last_write != None:
                    if last_write.transaction != op.transaction:
                        reads_from[res].append(ReadsFrom(last_write, op))
        return reads_from

    def final_writes(self) -> dict:
        return {res: next(
            reversed(
                list(
                    filter(lambda x: x.type == 'w', res_ops))), None)
                    for res, res_ops in self.resources_table().items()}
    
    def view_equivalent(self, sched2) -> bool:
        same_items = list_eqv(self.sched, sched2.sched)
        # Verify that all 
        if not same_items:
            return False
        resrcs = self.resources_set()
        fw1, fw2 = self.final_writes(), sched2.final_writes()
        same_final_writes = all(map(lambda res: fw1[res] == fw2[res], resrcs))
        if not same_final_writes:
            return False
        rf1, rf2 = self.reads_from(), sched2.reads_from()
        same_reads_from = all(map(lambda res: list_eqv(rf1[res], rf2[res]),resrcs))
        if not same_reads_from:
            return False
        else:
            return True
    
    def serial(self, transaction_perm : list):
        trans = self.transactions()
        sched = []
        for tr in transaction_perm:
            sched += trans[tr]
        return Schedule(op_list=sched)
    
    def is_serial(self):
        closed = set()
        current = None
        for op in self.sched:
            if op != current:
                if op.transaction in closed:
                    return False
                if current != None:
                    closed.add(current)
                current = op.transaction
        return True

    def VSR(self):
        return next(
            filter(lambda sched: self.view_equivalent(sched),
                map(lambda perm: self.serial(perm), itertools.permutations(list(self.transactions())))), None)
    
    def CSR(self):
        graph = nx.DiGraph()
        graph.add_nodes_from(self.transactions().keys())
        for ops in self.resources_table().values():
            confl = [(start_t.transaction, end_t.transaction)
                for i, start_t in enumerate(ops)
                    for j, end_t in enumerate(ops)
                        if conflicts(start_t,end_t) and i < j]
            graph.add_edges_from(confl)
        if not nx.is_directed_acyclic_graph(graph):
            return None
        else:
            return self.serial(list(nx.topological_sort(graph)))

def conflicts(op1, op2):
    return (op1.type == 'w' or op2.type == 'w') and op1.resource == op2.resource and op1.transaction != op2.transaction          

def list_eqv(l1: list, l2: list) -> bool:
    return Counter(l1) == Counter(l2)   

def read_schedule(sched_str: str) -> list:
    pieces = sched_str.split(" ")
    return [read_operation(piece) for piece in pieces]

def read_operation(op_str: str, trans_id=None) -> Operation :
    if re.match('[rw]\\d+(\\S+)', op_str):
        pieces = op_str.split('(')
        return Operation(pieces[0][0], int(pieces[0][1:]), pieces[1][:1])
    elif re.match('[rw](\\S+)', op_str) and trans_id != None:
        pieces = op_str.split('(')
        return Operation(pieces[0][0], int(trans_id), pieces[1][:1])
    else:
        raise ValueError('Malformed operation')

if __name__ == '__main__':
    pass