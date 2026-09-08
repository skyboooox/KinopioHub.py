from kinopio_hub._election import MeshElection, host_members


def member(name, host=None, broker=None, cpu=0):
    return {'id': name, 'hostId': host or name, 'vote': None, 'load': {'cpu': cpu, 'memory': 0}, 'observations': {}, 'broker': broker}


def connect(members):
    for voter in members:
        voter['observations'] = {m['id']: {'rtt': 1, 'loss': 0} for m in members}


def test_duplicate_host_one_delegate():
    members = [member('b', 'host'), member('a', 'host'), member('c')]
    assert [m['id'] for m in host_members(members)] == ['a', 'c']
    result = MeshElection('a').evaluate(members, 0)
    assert result['winner'] == 'a'
    assert sum(result['votes'].values()) == 1


def test_load_handoff_requires_term_and_three_rounds():
    members = [member('a', broker={'port': 1}, cpu=1), member('b')]
    connect(members)
    election = MeshElection('b')
    assert election.evaluate(members, 0)['vote'] == 'a'
    assert election.evaluate(members, 44999)['vote'] == 'a'
    assert election.evaluate(members, 45000)['vote'] == 'a'
    assert election.evaluate(members, 45001)['vote'] == 'a'
    assert election.evaluate(members, 45002)['vote'] == 'b'


def test_partition_leaders_merge_deterministically():
    members = [member('b', broker={'port': 1}), member('a', broker={'port': 2})]
    connect(members)
    assert MeshElection('a').evaluate(members, 0)['winner'] == 'a'
    assert MeshElection('b').evaluate(members, 0)['winner'] == 'a'


def test_coverage_overrides_better_load_and_failed_candidates():
    members = [member('a'), member('b', cpu=1), member('c')]
    for voter in members:
        voter['observations'] = {'b': {'rtt': 1, 'loss': 0}}
    assert MeshElection('a').evaluate(members, 0)['winner'] == 'b'
    members[1]['brokerFailed'] = True
    assert MeshElection('a').evaluate(members, 0)['winner'] != 'b'
