import { tableize } from 'inflected';
import { RecordIdentity } from '@orbit/data';

export function tableizeJoinTable(table1: string, table2: string) {
  return [tableize(table1), tableize(table2)].sort().join('_');
}

export function castAttributeValue(value: unknown, type?: string) {
  const typeOfValue = typeof value;
  const isString = typeOfValue === 'string';
  const isNumber = typeOfValue === 'number';
  if (type === 'boolean') {
    return value === 1;
  } else if (type === 'datetime' && (isString || isNumber)) {
    return new Date(value as string | number);
  }
  return value;
}

export function groupIdentitiesByType(identities: RecordIdentity[]) {
  const idsByType: Record<string, string[]> = {};
  for (let identity of identities) {
    idsByType[identity.type] = idsByType[identity.type] || [];
    idsByType[identity.type].push(identity.id);
  }
  return idsByType;
}
