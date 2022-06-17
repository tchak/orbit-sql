import { tableize } from 'inflected';
import { RecordIdentity } from '@orbit/records';

export function tableizeJoinTable(table1: string, table2: string) {
  return [tableize(table1), tableize(table2)].sort().join('_');
}

export function castAttributeValue(value: unknown, type?: string) {
  const typeOfValue = typeof value;
  const isString = typeOfValue === 'string';
  const isNumber = typeOfValue === 'number';
  if (type === 'boolean') {
    return Boolean(value);
  } else if (type === 'datetime' && (isString || isNumber)) {
    return new Date(value as string | number);
  }
  return value;
}

export function groupRecordsByType(records: RecordIdentity[]) {
  const recordsByType: Record<string, string[]> = {};
  for (let identity of records) {
    recordsByType[identity.type] = recordsByType[identity.type] || [];
    recordsByType[identity.type].push(identity.id);
  }
  return recordsByType;
}
