#!/usr/bin/python3
import sys, time, os
import classad
import htcondor
import json
import pprint
import copy


JobsAd_metrics = [
('RequestCpus', 'The number of CPUs requested for this job.' ),
('MachineAttrSlotWeight0', 'MachineAttrSlotWeight'),
('MachineAttrScalingFactorHEPSPEC060', 'HEPSPEC060 Scaling factor'),
('RequestMemory', 'The amount of memory space in MiB requested for this job.'), 
('maxWallTime', 'maxWallTime'),
# ('LastRemoteWallClockTime', 'Number of seconds the job was allocated a machine for its most recent completed execution.'),
('RemoteWallClockTime', 'Cumulative number of seconds the job has been allocated a machine.'),
('JobCurrentStartDate', 'Time at which the job most recently began running.'),
('RequestDisk', 'The amount of disk space in KiB requested for this job.'), 
('RemoteSysCpu', 'The total number of seconds of system CPU time (the time spent at system calls) the job used on remote machines.'),
# ('CumulativeRemoteUserCpu', 'system CPU time in seconds the job used on remote machines summed over all execution attempts.'),
('MemoryProvisioned', 'The amount of memory in MiB allocated to the job.'),
('CPUsUsage', 'Floating point value that represents the number of cpu cores fully used over the lifetime of the job.'),
('DiskProvisioned', 'The amount of disk space in KiB allocated to the job.'),
('DiskUsage', 'Amount of disk space (KiB) in the HTCondor execute directory on the execute machine that this job has used.'),
('RunTime', 'Difference between start and finish linux time.')
]

JobsComposedMetrics = [
    ('hs06requestcpus', 'MachineAttrScalingFactorHEPSPEC060 * requestcpus'),
    ('hs06walltime', 'runtime * hs06requestcpus'),
    ('hs06cputime', 'RemoteSysCpu * hs06requestcpus')
]

#wallclocktime ubehly cas * pocetcpu
#cputime 

# job

# pslot

JobsAd_labels = ['JobId', 'JobStatus', 'Scheduler', 'Owner','AcctGroup', 'AccountingGroup', 'RemoteHost', 
'x509UserProxyVOName', 'Queue']

Internal_Job_labels = ('JobId','Scheduler','Queue', 'RunTime')

partitionableslot_metrics = [
('TotalSlotCpus', 'The number of CPUs (cores) in this slot.'),
('Cpus', 'Remaining number of cpus in the partitionable slot'),
# ('TotalCondorLoadAvg', 'The load average contributed by HTCondor including allslots on the macine, remotejobs, benchmarks.'),
('TotalSlotMemory', 'The quantity of RAM in MiB given to this slot.'),
('Memory', 'The quantity of RAM remaining in the partitionable slot'),
('VirtualMemory', 'The amount of currently available virtual memory (swap space) expressed in KiB.'),
('TotalSlotDisk', 'Total number of disk space'),
('Disk', 'Remaining number of disk space in KiB for partitionable slot'),
('ExpectedMachineGracefulDrainingBadput', 'cpu-seconds lost if graceful draining was initiated now.'),
('ExpectedMachineGracefulDrainingCompletion', 'estimated linux time of complete graceful draining if it was initiated now.'),
('ExpectedMachineQuickDrainingBadput', 'cpu-seconds lost if quick of fast drain was initiated now'),
('ExpectedMachineQuickDrainingCompletion', 'estimated linux time of complete quick of fast drain if it was initiated now.'),
]

#Name here is the variable which is RemoteHost in jobs
partitionableslot_labels = ['Name', 'Activity', 'State', 'Machine', 'MachineOwner', 'StartJobs']



# IF IT IS ADDRESSED TO THE condor.farm.particle.cz THAN IT IS LOCAL JOB.
# konverze jobstatus z cislice na jmeno

jobs = {}
metrics = []

partitionable_slots = {}

QUEUE_NAMES = ["ops", "alice", "auger", "atlas", "atlassc", "atlasmc", "cta", "nova", "dune", "local", "unknown",
               "local_alice", "local_atlas", "local_auger", "local_fermilab", "local_hiprio", "local_user",
               "local_unknown"]

order_add = "group_atlas group_alice group_auger group_fermilab group_other_vo group_hiprio group_user group_test"

statusStrings = ['Unexpanded',
                 'Idle',
                 'Running',
                 'Removed',
                 'Completed',
                 'Held',
                 'Submission_err'
                ]

def collect_metrics_labels(job_metrics, job_labels):
    metrics = []
    for metric in job_metrics:
       metrics.append(metric[0])
    jobattributes = metrics + job_labels
    return jobattributes

def iterate_jobads_write_jobdict(schedulers, attributes, jobsdict):
    # retry = 2
    # while retry > 0:
        # retry -= 1
        try:
            for scheddad in schedulers:
              schedd = htcondor.Schedd(scheddad)
              for jobAd in schedd.query('true',[]):
                GlobalJobId = get_condor_value(jobAd, 'GlobalJobId', None)
                JobId = GlobalJobId.split('#')[1]
                if JobId not in jobsdict.keys():
                    jobsdict[JobId] = {}
                jobsdict[JobId]['Scheduler'] = GlobalJobId.split('#')[0]
                jobsdict[JobId]['JobId'] = GlobalJobId.split('#')[1]
                if GlobalJobId.split('#')[0] == 'condor.farm.particle.cz':
                       jobsdict[JobId]['local'] = True
                else:
                       jobsdict[JobId]['local'] = False

                iterate_ads_query_job_attributes(jobAd, jobsdict, attributes, JobId)

                runtime = 0
                if jobsdict[JobId]['JobStatus'] == 'Running':
                   runtime = get_condor_value(jobAd, 'ServerTime', 0) - get_condor_value(jobAd, 'JobStartDate', 0)
                lowcpu = False
                if runtime > 3600 and jobsdict[JobId]['CPUsUsage'] < 0.1:
                      lowcpu = True
                jobsdict[JobId]['LowCPU'] = lowcpu
                jobsdict[JobId]['RunTime'] = runtime
                # make_composed_metrics(jobsdict, JobId)
                #composed metrics
                jobsdict[JobId]['hs06requestcpus'] = (
                jobsdict[JobId]['MachineAttrScalingFactorHEPSPEC060'] * jobsdict[JobId]['RequestCpus']
                )
                jobsdict[JobId]['hs06walltime'] = jobsdict[JobId]['RunTime'] * jobsdict[JobId]['hs06requestcpus']
                jobsdict[JobId]['hs06cputime'] = jobsdict[JobId]['RemoteSysCpu'] * jobsdict[JobId]['hs06requestcpus']
            # break
        except Exception as e:
            ex = e

    # if not retry > 0:
        # print(jobsdict)
        # raise Exception('Exceded')




def iterate_histads_write_jobdict(schedulers, attributes, jobsdict):
    # retry = 2
    # while retry > 0:
    #     retry -= 1
        try:   
            for scheddad in schedulers:
              schedd = htcondor.Schedd(scheddad)
              for histAd in schedd.history(
                'true', [], -1, "CompletionDate < %s" % int(time.time() - 5 * 60)):
                
                GlobalJobId = get_condor_value(histAd, 'GlobalJobId', None)
                JobId = GlobalJobId.split('#')[1]
                if JobId not in jobsdict.keys():
                    jobsdict[JobId] = {}
                jobsdict[JobId]['Scheduler'] = GlobalJobId.split('#')[0]
                jobsdict[JobId]['JobId'] = GlobalJobId.split('#')[1]
                if GlobalJobId.split('#')[0] == 'condor.farm.particle.cz':
                       jobsdict[JobId]['local'] = True
                else:
                       jobsdict[JobId]['local'] = False
                
                iterate_ads_query_job_attributes(histAd, jobsdict, attributes, JobId)
                
                runtime = get_condor_value(histAd, 'JobFinishedHookDone', 0) - get_condor_value(histAd, 'JobStartDate', 0)
                jobsdict[JobId]['RunTime'] = runtime
                #composed metrics
                jobsdict[JobId]['hs06requestcpus'] = (
                     jobsdict[JobId]['MachineAttrScalingFactorHEPSPEC060'] * jobsdict[JobId]['RequestCpus']
                )
                jobsdict[JobId]['hs06walltime'] = jobsdict[JobId]['RunTime'] * jobsdict[JobId]['hs06requestcpus']
                jobsdict[JobId]['hs06cputime'] = jobsdict[JobId]['RemoteSysCpu'] * jobsdict[JobId]['hs06requestcpus']
            # break
        except Exception as e:
            ex = e

    # if not retry > 0:
    #     raise Exception('Exceded')
        


def iterate_ads_query_job_attributes(ad, jobsdict, attributes, JobId):
    for attribute in attributes:
        return_value = 0
        if attribute in ('CPUsUsage', 'RequestCpus', 'RequestMemory',
                          'maxWallTime', 'CumulativeRemoteUserCpu',
                          'LastRemoteWallClockTime'):
           return_value = 0.0
        elif attribute in ('MemoryProvisioned', 'DiskProvisioned', 'JobCurrentStartDate',
                           'MachineAttrSlotWeight0', 'x509UserProxyVOName', 'Owner'
                           'RemoteHost', 'RemoteWallClockTime'):
            return_value = None
        elif attribute == 'MachineAttrScalingFactorHEPSPEC060':
            return_value = 10.56
        if attribute in Internal_Job_labels:
            continue
        else:
            jobsdict[JobId][attribute] = get_condor_value(ad, attribute, return_value)

    
    #altering attributes and additional attributes
    q, cores = get_job_category(ad)
    jobsdict[JobId]['Queue'] = q
    jobsdict[JobId]['Cores'] = cores
    jobsdict[JobId]['AcctGroup'] = jobsdict[JobId]['AcctGroup'].split('.')[0]
    accounting_group = jobsdict[JobId]['AccountingGroup'].split(sep='.', maxsplit=2)
    jobsdict[JobId]['AccountingGroup'] = accounting_group[0].lstrip('group_') + '.' + accounting_group[1]
    jobsdict[JobId]['JobStatus'] = statusStrings[jobsdict[JobId]['JobStatus']]
    if '@' in str(jobsdict[JobId]['RemoteHost']):
      jobsdict[JobId]['RemoteHost'] = jobsdict[JobId]['RemoteHost'].split('@')[1]
    elif jobsdict[JobId]['RemoteHost'] == 0:
        jobsdict[JobId]['RemoteHost'] = ''

# def make_composed_metrics(jobsdict, jobid):
#     for cmetric in JobsComposedMetrics:




def iterate_partitionable_slots(coll, attributes, slotsdict):
    for machine in coll.query(
       htcondor.AdTypes.Startd, 'SlotType == "Partitionable"', attributes):
        name = get_condor_value(machine, 'Name', None)
        slotsdict[name] = {}
        for attribute in attributes:
            return_value = 0
            if attribute == 'TotalSlotDisk':
               return_value = 0.0
            # elif attribute == 'MachineOwner':
            #     print(machine)
            #     print(str(machine.get(attribute, 0)))   
            elif attribute in ('Activity', 'State', 'Name',
                               'MachineOwner', 'Machine'):

                return_value = None
            
            slotsdict[name][attribute] = get_condor_value(machine, attribute, return_value)


def make_metrics(metrics_list, labels_list, data_dict, differentiator):
    # works with mandatory structures 
    # JobsAd_metrics, JobsAd_labels, jobs, metrics
    metric_dict = {}
    metric_output = ''
    for identificator in data_dict:
      if not isinstance(identificator, str):
          continue
      for metric, description in metrics_list:
        args_dict = {}
        for parameter in labels_list:
            args_dict[parameter] = data_dict[identificator][parameter]
        if metric == 'MachineAttrScalingFactorHEPSPEC060':
            metric_name = 'htcondor' + '_' + differentiator + '_' + 'scalingfactorhs060'
        else:
            metric_name = 'htcondor' + '_' + differentiator + '_' +  metric.lower()

        ## zde to generuji sam - jeste je potreba zjednodusit kod pomoci techto modelu 
        ## filter(lambda x: x[0] in acct_groups_queued.keys() + acct_groups_running.keys(), fairshare.items()))
        if data_dict[identificator][metric] == None:
            continue
        args_list=[]
        for v in args_dict.items():
            args_list.append(str(v[0]) + '=' + '\"' + str(v[1]) + '\"')
        args=''
        for s in args_list:
          if args == '':
            args += s
          else:
            args += ',' + s
        
        # print(args_dict)
        OUTLINE_TEMPLATE = "%s{%s} %s"
    
        metric_string = OUTLINE_TEMPLATE % (metric_name, args, data_dict[identificator][metric]) + '\n'
        
        if not metric_name in metric_dict.keys():
          metric_description = '# HELP ' + metric_name + ' ' + description + '\n' + '# TYPE ' + metric_name + ' gauge\n'
          
          metric_dict[metric_name] = {
              'description': metric_description,
              'metrics': [ metric_string ]
          }
        elif metric_name in metric_dict.keys():
          # print('tady')
          # print(metric_dict[metric_name])
          metric_dict[metric_name]['metrics'].append(metric_string)
    for met_name in metric_dict.keys():
      metric_output += metric_dict[met_name]['description']
      for met in metric_dict[met_name]['metrics']:
        metric_output += met  
    
    print(metric_output)



# PV and MAdam class and functions ##############################################

class Queue:
    total_sweight = 0
    total_run = 0
    total_job = 0
    total_qued = 0
    total_othe = 0
    total_started = 0
    total_stopped = 0

    def __init__(self, q_name):
        self.name = q_name
        self.queued = 0
        self.sweight = 0
        self.running = 0
        self.other = 0
        self.started = 0
        self.stopped = 0

    def add_running_weight(self, sweight):
        self.sweight += sweight
        Queue.total_sweight += sweight

    def add_running_job(self, job_cores=1):
        self.running += job_cores
        Queue.total_run += job_cores
        Queue.total_job += 1

    def add_queued_job(self):
        self.queued += 1
        Queue.total_qued += 1

    def add_job_with_other_status(self):
        self.other += 1
        Queue.total_othe += 1

    def add_job_started(self):
        self.started += 1
        Queue.total_started += 1

    def add_job_stopped(self):
        self.stopped += 1
        Queue.total_stopped += 1



queues = {}
for q in QUEUE_NAMES:
    queues[q] = Queue(q)

# def iterate_startd_ads_oldstyle(coll):
#     tot_cor = 0
#     off_cor = 0
#     tot_scl = 0
#     off_scl = 0
#     drain_cores = 0

#     tot_mem = 0
#     off_mem = 0

#     for startdAd in coll.query(htcondor.AdTypes.Startd, 'SlotType =!= "Dynamic"', #zde taham vsechny joby oproti puvodnim pouze statickym
#                                ['Name', 'TotalMemory', 'ChildMemory', 'StartdIpAddr', 'MyAddress',
#                                 'TotalSlotCpus', 'Draining', 'ChildCpus',
#                                'SlotType', 'PartitionableSlot', 'DynamicSlot',
#                                'ScalingFactorHEPSPEC06', 'StartJobs']):

#         cpus = startdAd.get('TotalSlotCpus', 1)
#         memory = startdAd.get('TotalMemory', 0)
#         # print(memory)
#         draining = startdAd.get('Draining', False)
#         startjobs = startdAd.get('StartJobs', False)
#         childcpus = startdAd.get('ChildCpus', [])
#         childmemory = startdAd.get('ChildMemory', [])
#         # print(childmemory)
#         scaling06 = startdAd.get('ScalingFactorHEPSPEC06', 10.56)
#         tot_mem += memory
#         tot_cor += cpus
#         tot_scl += sum(childcpus) * scaling06
#         if not startjobs:
#             off_cor += cpus - sum(childcpus)
#             off_scl += (cpus - sum(childcpus)) * scaling06
#             off_mem += memory - sum(childmemory)
#         elif draining and len(childcpus) > 0:
#             # slot is flagged "Draining" even thought all jobs was already
#             # retired, but no new job is currently available for this slot
#             # (this will be even more tricky if we try to do "clever" filling
#             # of drained slots where we wait some time for jobs with biggest
#             # requirements and gradually lover such requirement not to wait
#             # forever empty)
#             drain_cores += cpus - sum(childcpus)

#     # print("htcondor_total_cores %d" % tot_cor)
#     print("htcondor_draining_cores %d" % drain_cores)
#     print("htcondor_offline_cores %d" % off_cor)
    
#     print("htcondor_total_memory %d" % tot_mem)
#     print("htcondor_offline_memory %d" % off_mem)

def get_time_string(seconds):
    try:
        h = seconds / 3600
        m = (seconds - h * 3600) / 60
        return str(h).zfill(2) + ":" + str(m).zfill(2)
    except ValueError:
        return "Unknown"
    except Exception as e:
        pprint.pprint(e)
        return "Unknown"

def standardize_condor_job_id(job_id):
    submit_host, submit_job, tmp = str(job_id).split('#', 2)
    return "%s@%s" % (submit_job, submit_host.split('.')[0])        

def get_condor_value(ad, key, default):
    ret = ad.get(key, default)
    if (type(ret) == classad.ExprTree and str(ret) == "DiskUsage"): 
      ret = default
    if type(ret) == classad.classad.ExprTree:
        if 'ifthenelse' in str(ret) : ret = ret.eval()
        else: 
          ret = str(ret)
    if type(ret) == classad.ExprTree: ret = ret.eval()
    if type(ret) == classad.Value: ret = default  # classad.Value.Error, classad.Value.Undefined
    return ret


def get_job_category(jobAd):
    # map VOs
    q = 'local'
    if not jobAd.get('GlobalJobId', 'unknown').startswith('condor.farm.particle.cz'):
        q = jobAd.get('x509UserProxyVOName', 'local').lower()
    cores = jobAd.get('RequestCpus', 1)
    if type(cores) == classad.ExprTree: cores = cores.eval()
    if type(cores) == classad.Value: cores = 1  # classad.Value.Error, classad.Value.Undefined

    # FIXME: hack for nova and MC Atlas
    if q == 'fermilab':
        if jobAd.get('x509UserProxyFirstFQAN', '').startswith('/fermilab/nova/'):
            q = 'nova'
        elif jobAd.get('x509UserProxyFirstFQAN', '').startswith('/fermilab/dune/'):
            q = 'dune'
    elif q == 'vo.cta.in2p3.fr':
        q = 'cta'
    elif q == 'atlas':
        if jobAd.get('x509UserProxyFirstFQAN', '').startswith('/atlas/Role=production'):
            if cores > 1:
                q = 'atlasmc'
            else:
                q = 'atlassc'

    # map AcctGroup to local user groups
    if q == 'local':
        q = jobAd.get('AcctGroup', 'local_na').lower()
        if type(q) == classad.ExprTree: q = q.eval()
        if type(q) == classad.Value:
            q = 'local_error'  # classad.Value.Error, classad.Value.Undefined
        else:
            if q.startswith('group_'): q = q[len('group_'):]
            if q.find('.') != -1: q = q[:q.find('.')]
            q = "local_%s" % q
        if q not in QUEUE_NAMES: q = 'local_unknown'
    elif q not in queues:
        q = 'unknown'

    return (q, cores)

#### end of MAdam class and functions ########################################

coll = htcondor.Collector("htc.farm.particle.cz")

schedulers = coll.query(ad_type=htcondor.AdTypes.Schedd,constraint='true', projection=[])


job_params = [
        'MyType',
        'ClusterId', 'ProcId', 'GlobalJobId', 'JobStatus', 'AcctGroup',
        'Owner', 'User', 'x509UserProxyVOName', 'x509UserProxyFirstFQAN',
        'RequestCpus', 'RequestMemory', 'RequestDisk',
        'ServerTime', 'JobCurrentStartDate', 'CompletionDate',
        'MachineAttrCpus0', 'MachineAttrSlotWeight0',
        'MachineAttrScalingFactorHEPSPEC060',
]

iterate_jobads_write_jobdict(
    schedulers, 
    collect_metrics_labels(JobsAd_metrics, JobsAd_labels), 
    jobs
)


iterate_histads_write_jobdict(
    schedulers, 
    collect_metrics_labels(JobsAd_metrics, JobsAd_labels), 
    jobs
)

# print(json.dumps(jobs,indent=4))

make_metrics(JobsAd_metrics, JobsAd_labels, jobs, 'job')

make_metrics(JobsComposedMetrics, JobsAd_labels, jobs, 'job')

iterate_partitionable_slots(coll, collect_metrics_labels(
    partitionableslot_metrics, partitionableslot_labels), partitionable_slots)

make_metrics(partitionableslot_metrics, partitionableslot_labels, partitionable_slots, 'pslot')

# print(json.dumps(partitionable_slots, indent=4))

# print(collect_metrics_labels(partitionableslot_metrics, partitionableslot_labels))

