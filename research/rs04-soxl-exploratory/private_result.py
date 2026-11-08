"""Project a SOXL historical research result to an aggregate-only artifact."""
import math
import re

ARMS = ('B1.1', 'B1.2', 'B1.3', 'B1.4', 'B2.1', 'B2.2')
METRICS = frozenset(('net_log_growth_per_period', 'sharpe_per_period', 'probabilistic_sharpe_ratio',
    'deflated_sharpe_ratio', 'minimum_track_record_periods', 'cvar95_return_per_period', 'max_drawdown',
    'cost_log_drag_per_period', 'net_cagr', 'net_calmar_ratio', 'net_win_rate_ex_zero_per_period',
    'net_payoff_ratio_per_period', 'net_profit_factor_per_period', 'mean_net_return_per_period',
    'max_drawdown_peak_to_recovery_observations', 'max_drawdown_trough_to_recovery_observations'))
REASONS = frozenset(('REPLAY_REQUIRED_FIELD_MISSING', 'PAIRED_PATH_MISSING', 'INSUFFICIENT_OBSERVATIONS',
    'FILL_LEDGER_MISSING', 'PAIRED_SESSIONS_NOT_ALIGNED', 'NET_NAV_INVALID', 'NET_NAV_DISCONTINUITY',
    'PAIRED_INITIAL_NAV_MISMATCH', 'insufficient_observations', 'zero_variance', 'numerical_failure',
    'missing_risk_free_returns', 'missing_gross_returns', 'no_nonzero_observations', 'no_loss_observations',
    'no_win_observations', 'zero_drawdown', 'cagr_uncomputable', 'sharpe_not_above_benchmark'))


def _number(value):
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError('INVALID_AGGREGATE')
    return value


def _reason(value):
    return None if value is None else value if value in REASONS else 'OTHER_FAILURE'


def _period(period):
    if period is None:
        return None
    report = period['report']
    metrics = []
    for metric in report.get('metrics', []):
        if metric.get('name') in METRICS:
            status = metric['status']
            if status not in ('COMPUTED', 'UNCOMPUTABLE'):
                raise ValueError('INVALID_METRIC_STATUS')
            metrics.append({'name': metric['name'], 'status': status, 'value': _number(metric.get('value')),
                            'reason_code': _reason(metric.get('reason_code'))})
    execution = report.get('execution', {})
    risk = period.get('drawdown_observations', {})
    return {'sessions': _number(period['sessions']),
        'cumulative_net_return': _number(report.get('cumulative_net_return')),
        'annualized_net_volatility': _number(report.get('annualized_net_volatility')),
        'max_drawdown_with_path_peak': _number(period.get('max_drawdown_with_path_peak')),
        'metrics': metrics,
        'execution': {k: _number(execution.get(k)) for k in ('turnover_one_side', 'commission_usd', 'slippage_usd', 'total_cost_usd')},
        'drawdown_observations': {k: _number(risk.get(k)) for k in ('drawdown_limit', 'opening_eligible_sessions',
            'opening_observed_sessions', 'opening_gap_crossings', 'prefill_close_crossings', 'fee_crossings',
            'post_cost_sessions_below_floor')}}


def _scenario(row):
    arm, variant = row['arm'], row['variant']
    fees = tuple(row['fees'])
    if (row['profile'] != 'SOXL' or arm not in ARMS or variant not in ('registered', 'sensitivity')
            or (variant == 'sensitivity' and arm != 'B1.4') or fees not in ((0., 0.), (2., 5.), (5., 10.))
            or row['scenario_id'] != f'SOXL/{arm}/{variant}/{sum(fees):g}bps'
            or row['status'] not in ('COMPUTED', 'FAILED')):
        raise ValueError('INVALID_SCENARIO')
    return {'scenario_id': row['scenario_id'], 'profile': 'SOXL', 'arm': arm, 'variant': variant,
        'fees': list(fees), 'ewma_decay': _number(row.get('ewma_decay')),
        'target_annual_vol': _number(row.get('target_annual_vol')), 'status': row['status'],
        'reason_code': _reason(row.get('reason_code')), 'completed_sessions': _number(row['completed_sessions']),
        'main': _period(row.get('main')), 'tail': _period(row.get('tail'))}


def _pair(row):
    if (row['profile'] != 'SOXL' or (row['candidate'], row['control']) not in (('B1.4', 'B1.2'), ('B2.2', 'B2.1'))
            or row['status'] not in ('PARKED', 'COMPUTED', 'DESCRIPTIVE_ONLY') or row['fee_bps'] != 7
            or type(row['positive_increment_supported']) is not bool):
        raise ValueError('INVALID_COMPARISON')
    out = {k: row[k] for k in ('profile', 'candidate', 'control', 'fee_bps', 'status', 'positive_increment_supported')}
    out['reason_code'] = _reason(row.get('reason_code'))
    out['mean_daily_net_log_delta'] = _number(row.get('mean_daily_net_log_delta'))
    for key in ('ci95', 'ci98_75'):
        value = row.get(key)
        if value is not None and len(value) != 2:
            raise ValueError('INVALID_INTERVAL')
        out[key] = None if value is None else [_number(v) for v in value]
    return out


def project_result(result):
    """Allowlist aggregate data; the caller binds provenance and D10 before projection."""
    p = result['provenance']
    provenance = {}
    for key, length in (('source_sha', 40), ('manifest_sha256', 64), ('issuer_csv_sha256', 64),
                        ('plan_sha256', 64), ('archive_source_binding', 64)):
        value = p[key]
        if not isinstance(value, str) or not re.fullmatch('[0-9a-f]{%d}' % length, value):
            raise ValueError('INVALID_PROVENANCE')
        provenance[key] = value
    calendar = p['calendar_version']
    if not isinstance(calendar, str) or not re.fullmatch(r'[A-Za-z0-9_.+-]{1,64}', calendar):
        raise ValueError('INVALID_PROVENANCE')
    provenance['calendar_version'] = calendar
    d10 = result['d10']
    expected = {'basis': 'one_way_per_security_net_traded_notional_including_boxx',
        'components': ('commission_bps', 'slippage_including_spread_bps'), 'primary': (2., 5.),
        'report_only': ((0., 0.), (5., 10.)), 'fund_expenses': 'embedded_in_etf_returns', 'financing': 'no_borrowing'}
    if (d10.get('basis') != expected['basis'] or tuple(d10.get('components', ())) != expected['components']
            or tuple(d10.get('primary', ())) != expected['primary']
            or tuple(map(tuple, d10.get('report_only', ()))) != expected['report_only']
            or d10.get('fund_expenses') != expected['fund_expenses'] or d10.get('financing') != expected['financing']):
        raise ValueError('INVALID_D10')
    return {'scope': {'profile': 'SOXL', 'purpose': 'historical_development_only',
                      'strict_pit_verified': False, 'production_use_authorized': False},
        'provenance': provenance, 'd10': {**expected, 'components': list(expected['components']),
            'primary': list(expected['primary']), 'report_only': [list(x) for x in expected['report_only']]},
        'scenarios': [_scenario(row) for row in result['scenarios']],
        'primary': [_pair(row) for row in result['primary']],
        'descriptive_tail': [_pair(row) for row in result['descriptive_tail']]}
