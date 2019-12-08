'use strict'

import Datastore from './datastore'
import { KeyViolation, NotExists, NoIndex } from './errors'

Object.assign(Datastore, { KeyViolation, NotExists, NoIndex })
export default Datastore
