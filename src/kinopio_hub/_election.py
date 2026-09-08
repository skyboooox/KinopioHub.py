"""LAN votes coordinate convergence; they do not fence network partitions."""
from typing import Any

Member = dict[str, Any]


def host_members(members: list[Member]) -> list[Member]:
    hosts: dict[str, Member] = {}
    for member in sorted(members, key=lambda item: item['id']):
        hosts.setdefault(member['hostId'], member)
    return list(hosts.values())


def candidate_scores(members: list[Member]) -> dict[str, float]:
    voters = host_members(members)
    scores = {}
    for candidate in members:
        latency = loss = 0.0
        for voter in voters:
            sample = {'rtt': 0, 'loss': 0} if voter['id'] == candidate['id'] else voter.get('observations', {}).get(candidate['id'])
            latency += min(1, max(0, sample['rtt']) / 100) if sample else 1
            loss += min(1, max(0, sample['loss'])) if sample else 1
        count = max(1, len(voters))
        load, uplink = candidate.get('load') or {}, candidate.get('uplink') or {}
        uplink_cost = 0.3 if uplink.get('reachable') is False else min(1, (uplink.get('rtt') or 0) / 150) * 0.1 if uplink.get('reachable') is True else 0
        scores[candidate['id']] = uplink_cost + loss / count * 0.5 + latency / count * 0.3 + min(1, max(0, load.get('cpu', 0))) * 0.12 + min(1, max(0, load.get('memory', 0))) * 0.08
    return scores


class MeshElection:
    def __init__(self, member_id: str, *, minimum_term_ms: int = 45000, improvement: float = 0.08, improvement_rounds: int = 3):
        self.id = member_id
        self.minimum_term_ms, self.improvement, self.improvement_rounds = minimum_term_ms, improvement, improvement_rounds
        self.incumbent: str | None = None
        self.incumbent_since = 0.0
        self.challenger: str | None = None
        self.rounds = 0

    def evaluate(self, members: list[Member], now: float) -> dict[str, Any]:
        scores = candidate_scores(members)
        delegates = host_members(members)
        eligible = host_members([m for m in members if not m.get('brokerFailed') and m.get('unavailableUntil', 0) <= now])
        def coverage(member: Member) -> int:
            return sum(v['id'] == member['id'] or v.get('observations', {}).get(member['id'], {}).get('loss', 1) < 0.5 for v in delegates)
        def uplink(member: Member) -> bool:
            return (member.get('broker') or {}).get('upstreamConnected') is True or (member.get('uplink') or {}).get('reachable') is True
        ordered = sorted(eligible, key=lambda m: (-coverage(m), -int(uplink(m)), scores[m['id']], m['id']))
        leaders = sorted([m for m in members if m.get('broker') and not m.get('brokerFailed')], key=lambda m: m['id'])
        incumbent = leaders[0]['id'] if leaders else None
        if self.incumbent != incumbent:
            self.incumbent, self.incumbent_since, self.challenger, self.rounds = incumbent, now, None, 0
        best = ordered[0]['id'] if ordered else None
        vote = incumbent or best
        if incumbent and best and best != incumbent and now - self.incumbent_since >= self.minimum_term_ms and (coverage(ordered[0]) > coverage(next(m for m in members if m['id'] == incumbent)) or scores[incumbent] - scores[best] >= self.improvement):
            self.rounds = self.rounds + 1 if self.challenger == best else 1
            self.challenger = best
            if self.rounds >= self.improvement_rounds:
                vote = best
        else:
            self.challenger, self.rounds = None, 0
        votes: dict[str, int] = {}
        allowed = {m['id'] for m in eligible + leaders}
        for member in delegates:
            choice = vote if member['id'] == self.id else member.get('vote')
            if choice and choice in allowed:
                votes[choice] = votes.get(choice, 0) + 1
        tally = sorted(votes, key=lambda member_id: (-votes[member_id], member_id))
        majority = tally[0] if tally and votes[tally[0]] > len(delegates) / 2 else None
        return {'vote': vote, 'winner': majority or incumbent or (tally[0] if tally else best), 'scores': scores, 'votes': votes}
