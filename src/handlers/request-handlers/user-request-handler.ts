import { getMasterDbClient, getReadReplicaDbClient } from '../../database/client'
import { Request, Response } from 'express'
import { createLogger } from '../../factories/logger-factory'
import { createSettings } from '../../factories/settings-factory'
import { UserRepository } from '../../repositories/user-repository'

const debug = createLogger('user-request-handler')

export const userRequestHandler = async (req: Request, res: Response) => {
  const authkey = process.env.RELAY_API_KEY;
  if (!authkey || !req.query.token) {
    return res.status(403).send('Unauthorized');
  }
  if (!req.query.pubkey) {
    return res.status(400).send('Must send pubkey in query')
  }
  const dbClient = getMasterDbClient()
  const userRepository = new UserRepository(dbClient)

  var balance = await userRepository.getBalanceByPubkey(req.query.pubkey.toString());
  if (!balance) {
    return res.status(404).send('Pubkey not found');
  }
  return res.status(200).json({balance: Number(balance)});
}

