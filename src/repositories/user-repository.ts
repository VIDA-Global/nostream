import { always, applySpec, omit, pipe, prop } from 'ramda'

import { DatabaseClient, Pubkey } from '../@types/base'
import { DBUser, User } from '../@types/user'
import { fromDBUser, toBuffer } from '../utils/transform'
import { createLogger } from '../factories/logger-factory'
import { Factory } from '../@types/base'
import { IUserRepository } from '../@types/repositories'
import { Settings } from '../@types/settings'
import { createSettings } from '../factories/settings-factory'
import httpClient, { CreateAxiosDefaults } from 'axios'
import { getCacheClient } from '../cache/client'

const debug = createLogger('user-repository')

export class UserRepository implements IUserRepository {
  private readonly cacheClient = getCacheClient();
  private readonly settings = createSettings();

  public constructor(
    private readonly dbClient: DatabaseClient,
  ) { }

  public async findByPubkey(
    pubkey: Pubkey,
    client: DatabaseClient = this.dbClient,
  ): Promise<User | undefined> {
    debug('find by pubkey: %s', pubkey)

    //Check cache for blocked pubkey
    console.log(`Checking is-blocked cache for pubkey ${pubkey}`);
    const blocked = await this.cacheClient.exists(`${pubkey}:is-blocked`);
    if (blocked) {
      console.log(`PubKey ${pubkey} is blocked`);
      return;
    }

    //Check if user is stored locally already
    const [dbuser] = await client<DBUser>('users')
        .where('pubkey', toBuffer(pubkey))
        .select();

    if (!dbuser) {
      //If enabled, fetch from webhook
      const webhookUser = await this.fetchUserByWebhook(pubkey);
      if (webhookUser) {
        console.log(`Received response from webhook`)
        console.log(`Storing user locally`)
        const date = new Date();
        await this.upsert(
          {
            pubkey: webhookUser.pubkey,
            isAdmitted: webhookUser.isAdmitted,
            balance: BigInt(webhookUser.balance),
            createdAt: date,
            tosAcceptedAt: date,
            updatedAt: date,
          },
          //transaction.transaction,
        )
        return webhookUser;
      } else {
        //No user found
        //Store in cache for temp blocking so we don't keep hitting api/db
        console.log(`Setting block in cache for ${pubkey}`)
        await this.cacheClient.set(`${pubkey}:is-blocked`, `true`);
        await this.cacheClient.expire(`${pubkey}:is-blocked`, 60);
        return
      }
    } else {
      console.log('Found user from local db')
      return fromDBUser(dbuser)
    }
  }

  public async upsert(
    user: User,
    client: DatabaseClient = this.dbClient,
  ): Promise<number> {
    debug('upsert: %o', user)

    const date = new Date()

    const row = applySpec<DBUser>({
      pubkey: pipe(prop('pubkey'), toBuffer),
      is_admitted: prop('isAdmitted'),
      balance: prop('balance'),
      tos_accepted_at: prop('tosAcceptedAt'),
      updated_at: always(date),
      created_at: always(date),
    })(user)

    const query = client<DBUser>('users')
      .insert(row)
      .onConflict('pubkey')
      .merge(
        omit([
          'pubkey',
          'balance',
          'created_at',
        ])(row)
      )

    return {
      then: <T1, T2>(onfulfilled: (value: number) => T1 | PromiseLike<T1>, onrejected: (reason: any) => T2 | PromiseLike<T2>) => query.then(prop('rowCount') as () => number).then(onfulfilled, onrejected),
      catch: <T>(onrejected: (reason: any) => T | PromiseLike<T>) => query.catch(onrejected),
      toString: (): string => query.toString(),
    } as Promise<number>
  }

  public async getBalanceByPubkey(
    pubkey: Pubkey,
    client: DatabaseClient = this.dbClient
  ): Promise<bigint> {
    debug('get balance for pubkey: %s', pubkey)

    const [user] = await client<DBUser>('users')
      .select('balance')
      .where('pubkey', toBuffer(pubkey))
      .limit(1)

    if (!user) {
      return 0n
    }

    return BigInt(user.balance)
  }

  protected async fetchUserByWebhook(
    pubkey: Pubkey
  ): Promise<User | undefined> {

    /*
      Generates a POST req with body:
        {
          pubkey: 'cb46e9...',  //pubkey (hex)
          amount: 500  //mSat min required
        }

      Expects response:
      {
        pubkey: 'cb46e9...',
        isAdmitted: true,
        credit: 5000, //in mSats
        createdAt: 1675727291,
        updatedAt: 1675727387
      }
    */
    if(!this.settings.webhooks?.pubkeyChecks || !this.settings.webhooks?.endpoints?.baseURL || !this.settings.webhooks?.endpoints?.pubkeyCheck) {
      return;
    }
    if (!process.env.VIDA_API_KEY) {
      console.log('Unable to find Vida API Key');
      return;
    }
    const url = `${this.settings.webhooks?.endpoints?.baseURL}${this.settings.webhooks?.endpoints?.pubkeyCheck}?token=${process.env.VIDA_API_KEY}`;
    try {
        // send a POST to the endpoint with the pubKey and minimum balance. endpoint will basically return true/false
      const body = {
        pubkey: pubkey,
        amount: this.settings.payments?.feeSchedules?.topUp[0].amount || 0
      }
      const response = await httpClient.post(url, body, {
        maxRedirects: 1,
      })
      if (response && response.data?.isAdmitted) {
        return {
          pubkey: pubkey,
          isAdmitted: response.data.isAdmitted,
          balance: BigInt(response.data.balance),
          createdAt: new Date(response.data.createdAt) || new Date(Date.now()),
          updatedAt: new Date(response.data.updatedAt) || new Date(Date.now()),
        }
      } else if (response && !response.data?.isAdmitted) {
        console.log(`Received negative isAdmitted response from webhook. Rejecting.`)
        return;
      } 
      console.log(`Didn't receive response from webhook for isAdmitted check`);
      return;

    } catch (e) {
      debug(`Unable to fetch remote pubkey from webhook endpoint ${url}`);
      throw e;
      return;      
    }

  }

  public async topUpPubkey(
    pubkey: Pubkey
  ): Promise<boolean> {

    /*
      Generates a POST req with body:
        {
          pubkey: 'cb46e9...',  //pubkey (hex)
          
        }

      Expects response:
      {
        success: true || false
      }
    */
    console.log('Topping up user by webhook');
    if(!this.settings.webhooks?.topUps || !this.settings.webhooks?.endpoints?.baseURL || !this.settings.webhooks?.endpoints?.topUps) {
      return false;
    }
    if (!process.env.VIDA_API_KEY) {
      console.log('Unable to find API Key');
      return false;
    }
    const amount = this.settings.payments?.feeSchedules?.topUp[0].amount || BigInt(0);
    console.log(`Topping up ${pubkey} by amount ${amount}`)

    const url = `${this.settings.webhooks?.endpoints?.baseURL}${this.settings.webhooks?.endpoints?.topUps}?token=${process.env.VIDA_API_KEY}`;
    try {
        // send a POST to the endpoint with the pubKey and minimum balance. endpoint will basically return true/false
      const body = {
        pubkey: pubkey,
        amount: amount
      }
      const response = await httpClient.post(url, body, {
        maxRedirects: 1,
      })

      if (response && response.data?.success) {
        console.log(`Topped up ${pubkey} successfully`)
        await this.incrementUserBalance(pubkey, amount);
        return true;
      } else {
        console.log('Did not receive a response or success from topup webhook ep')
        return false;
      }

    } catch (e) {
      debug(`Unable to process topup from webhook endpoint`);
      throw e;
      return false;      
    }
  }

  public async incrementUserBalance(
    pubkey: Pubkey,
    amount: bigint,
    client: DatabaseClient = this.dbClient
  ): Promise<number> {
    debug('incrementUserBalance: %o', pubkey)
    console.log(`Incrementing User Balance for ${pubkey} by amount ${amount}`);
    const queryRaw = `UPDATE users SET balance = balance + ${amount} WHERE pubkey = '\\x${pubkey}'`;
    const query = await client.raw(queryRaw);

    return
  }

  public async decrementUserBalance(
    pubkey: Pubkey,
    amount: bigint,
    client: DatabaseClient = this.dbClient
  ): Promise<number> {
    debug('decrementUserBalance: %o', pubkey)
    console.log(`Decrementing User Balance for ${pubkey} by amount ${amount}`);
    const queryRaw = `UPDATE users SET balance = balance - ${amount} WHERE pubkey = '\\x${pubkey}'`;
    const query = await client.raw(queryRaw);

    return
  }

}
