
conf = {'bootstrap.servers': 'localhost:9092'}
conf['group.id'] = 'business_domain.default'

business_domain = BusinessDomain(conf)

profile = {
    'partition_by_domain': {
        'number': 1,
        'kafka-procedure': {
            'in': topic.ENQUEUE,
            'out': [topic.PARTITIONED, topic.PARTITIONED],
            'func': business_domain.partition_by_domain,
            'conf': conf
        }

    },
    'fragment_by_length': {
        'number': 1,
        'kafka-procedure': {
            'in': topic.PARTITIONED,
            'out': [topic.FRAGMENTED, topic.PARTITIONED],
            'func': business_domain.fragment_by_length,
            'conf': conf
        }

    },
    'request_accounting': {
        'number': 1,
        'kafka-procedure': {
            'in': topic.FRAGMENTED,
            'out': [],
            'func': business_domain.request_accounting,
            'conf': conf
        }

    },
    'update_ap_db': {
        'number': 1,
        'kafka-procedure': {
            'in': topic.ACCOUNTING_DONE,
            'out': [],
            'func': business_domain.update_ap_db,
            'conf': conf
        }

    },
}
