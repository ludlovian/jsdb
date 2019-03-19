'use strict'

import Datastore from './datastore'
import { KeyViolation, NotExists } from './errors'

Object.assign(Datastore, { KeyViolation, NotExists })
export default Datastore
