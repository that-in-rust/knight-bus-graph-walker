"""Paid canonical-class validation for a closed-form Boolean spectral bound.

Trusted prepared rows/degree/ID invariants remain the source's responsibility.
The check retains only constant current-row metadata, not a list of pairs.
"""

from fractions import Fraction

from stream_boolean_rank_solver import manage_source_iterator_lifetime


def validate_boolean_spectral_source(source, *, max_factor_slots):
    factors = source.factor_count
    if (type(max_factor_slots) is not int or max_factor_slots < 0
            or type(factors) is not int or not 4 <= factors <= max_factor_slots):
        raise ValueError("uniform pair source requires F>=4 within factor reservation")
    classes = factors * (factors - 1) // 2
    for value in (source.class_count, source.active_class_count, source.vertex_count):
        if type(value) is not int or value <= 0:
            raise ValueError("invalid uniform pair source count")
    if source.class_count != classes or source.active_class_count != classes:
        raise ValueError("source must contain every pair class and no other classes")
    if not isinstance(source.total_weight, Fraction) or source.total_weight <= 0:
        raise ValueError("positive exact total weight required")
    if source.isolate_weight != 0:
        raise ValueError("uniform pair source cannot contain isolates")
    snapshot = source.snapshot_id
    left, right, count, height = 0, 1, 0, None
    total = Fraction()
    with manage_source_iterator_lifetime(source.iterate_class_records()) as records:
        for class_id, groups, h, degree, weight in records:
            if (type(class_id) is not int or class_id != count or count >= classes
                    or type(groups) is not tuple or groups != (left, right)
                    or any(type(group) is not int for group in groups)):
                raise ValueError("class stream must be the canonical complete pair sequence")
            if type(h) is not int or not 1 <= h <= 2**53:
                raise ValueError("invalid uniform class height")
            if height is None:
                height = h
            if (h != height or type(degree) is not int
                    or degree != h * (2 * factors - 3) - 1 or degree > 2**53):
                raise ValueError("nonuniform height or inconsistent Boolean degree")
            if not isinstance(weight, Fraction) or weight < 0:
                raise ValueError("nonnegative exact class weight required")
            total += weight
            count += 1
            right += 1
            if right == factors:
                left += 1
                right = left + 1
    if (count != classes or height is None or height * classes != source.vertex_count
            or total != source.total_weight or snapshot != source.snapshot_id):
        raise ValueError("incomplete or inconsistent pinned uniform pair source")
    degree = height * (2 * factors - 3) - 1
    return dict(factors=factors, height=height, vertices=source.vertex_count,
                degree=degree, class_count=classes, snapshot_id=snapshot,
                spectral_upper=Fraction(height * (factors - 3) - 1, degree),
                validation_class_rows=count)
