import assert from 'node:assert/strict';
import { describe, it } from 'node:test';

import { BaileysStartupService } from '../src/api/integrations/channel/whatsapp/whatsapp.baileys.service';
import { WAMonitoringService } from '../src/api/services/monitor.service';

describe('WhatsApp connection lifecycle', () => {
  it('finishes logout when Baileys reports an already closed socket', async () => {
    const calls: string[] = [];
    const context = {
      isDeleting: false,
      endSession: false,
      messageProcessor: { onDestroy: () => calls.push('processor.destroy') },
      client: {
        logout: async () => {
          calls.push('client.logout');
          throw new Error('Connection Closed');
        },
        ws: { close: () => calls.push('socket.close') },
        end: () => calls.push('client.end'),
      },
      logger: { warn: () => calls.push('logger.warn') },
      stateConnection: { state: 'open' },
      instance: { id: 'instance-id', qrcode: { count: 4 } },
      instanceId: 'instance-id',
      instanceName: 'playground',
      prismaRepository: {
        instance: {
          update: async ({ data }: any) => {
            calls.push(`instance.update:${data.connectionStatus}`);
          },
        },
        session: { deleteMany: async () => calls.push('session.deleteMany') },
      },
      configService: {
        get: (key: string) => {
          if (key === 'DATABASE') return { SAVE_DATA: { INSTANCE: false } };
          if (key === 'CACHE') return { REDIS: { ENABLED: false, SAVE_INSTANCES: false } };
          if (key === 'PROVIDER') return { ENABLED: false };
          return {};
        },
      },
    };

    await BaileysStartupService.prototype.logoutInstance.call(context as any);

    assert.equal(context.stateConnection.state, 'close');
    assert.equal(context.instance.qrcode.count, 0);
    assert.equal(context.isDeleting, true);
    assert.equal(context.endSession, true);
    assert.ok(calls.includes('logger.warn'));
    assert.ok(calls.includes('instance.update:close'));
    assert.ok(calls.includes('session.deleteMany'));
  });

  it('reconciles an open state to close when the Baileys socket is closed', async () => {
    const updates: any[] = [];
    const waInstance = {
      connectionStatus: { state: 'open' },
      stateConnection: { state: 'open', statusReason: 200 },
      client: { ws: { isOpen: false } },
    };
    const context = {
      waInstances: { playground: waInstance },
      prismaRepository: {
        instance: { update: async (args: any) => updates.push(args) },
      },
      logger: { warn: () => undefined },
    };

    const state = await WAMonitoringService.prototype.reconcileConnectionState.call(
      context as any,
      'playground',
      'open',
    );

    assert.equal(state, 'close');
    assert.equal(waInstance.stateConnection.state, 'close');
    assert.equal(updates.length, 1);
    assert.equal(updates[0].data.connectionStatus, 'close');
  });

  it('keeps an open state when the Baileys socket is healthy', async () => {
    const context = {
      waInstances: {
        playground: {
          connectionStatus: { state: 'open' },
          stateConnection: { state: 'open' },
          client: { ws: { isOpen: true } },
        },
      },
      prismaRepository: {
        instance: { update: async () => assert.fail('healthy state must not be rewritten') },
      },
      logger: { warn: () => undefined },
    };

    const state = await WAMonitoringService.prototype.reconcileConnectionState.call(
      context as any,
      'playground',
      'open',
    );

    assert.equal(state, 'open');
  });
});
