import * as fs from 'fs'
import {promisify} from 'util'
import {PickByValueExact, $Keys, $Values} from 'utility-types'

const readFile = promisify(fs.readFile)
const writeFile = promisify(fs.writeFile)

interface IEntity {
	_id: number
}

type QueryParam<Prop> = {$eq: Prop} |
                        {$gt: Prop} |
                        {$lt: Prop} |
                        {$in: Prop[]}

type Queryable<Entity> = Partial<{
	[K in keyof Entity]: QueryParam<Entity[K]>
}>

type Operators<Entity> = Partial<{
	$and: Queryable<Entity>[],
	$or: Queryable<Entity>[],
	$text: string,
}>

type Query<Entity> = Queryable<Entity> & Operators<Entity>

type QuerySort<Entity> = Partial<{[K in keyof Entity]: 1 | -1}>
type QueryProjection<Entity> = Partial<{[K in keyof Entity]: 1}>

type QueryOptions<Entity> = {
	sort?: QuerySort<Entity>
	projection?: QueryProjection<Entity>
	deleted?: boolean
}

type Deletable<Entity> = Entity & {exists: boolean}

export class Database<Entity extends object & IEntity, StringField = $Keys<PickByValueExact<Entity, string>>> {
		protected lock: Promise<void> = Promise.resolve();
    protected filename: string;
    protected fullTextSearchFieldNames: StringField[];

    constructor(filename: string, fullTextSearchFieldNames: StringField[]) {
        this.filename = filename;
        this.fullTextSearchFieldNames = fullTextSearchFieldNames;
    }

    async insert(entity: Entity): Promise<void> {
    	await this.flush([entity])
    }

    async find(query: Query<Entity>, opts: QueryOptions<Entity> = {}): Promise<Partial<Entity>[]> {
    	const entities = await this.load()
      return this.project(opts.projection, this.sort(opts.sort, this.filter(query, entities)))
    }

    async delete(query: Query<Entity>): Promise<void> {
    	const entities = await this.load()
    	await this.flush([], this.filter(query, entities))
    }

    // DATA READ / WRITE

    private async load(): Promise<Entity[]> {
    	const all = await this.loadAll()
    	return all
    		.filter(({exists}) => exists)
    		.map(({exists, ...rest}) => rest as Entity)
    }

    private async loadAll(): Promise<Deletable<Entity>[]> {
    	const data = await readFile(this.filename, 'utf8')
    	return data
    		.split('\n')
    		.filter(line => !!line)
    		.map((raw) => ({
    			...JSON.parse(raw.slice(1)),
    			exists: raw[0] === 'E',
    		}))
    }

    private async flush(inserts: Entity[], deletes: IEntity[] = []): Promise<void> {
    	this.lock = this.lock.then(async () => {
	    	const all = await this.loadAll()
	    	const deletedIds = deletes.map(({_id}) => _id)
	    	const newDeletables = inserts.map((e) => ({...e, exists: true}))
	    	const data = all
	    		.map((e) => deletedIds.includes(e._id) ? {...e, exists: false} : e)
	    		.concat(newDeletables)
	    		.reduce((acc, e) => `${acc}\n${toLine(e)}`, ``)

	    	await writeFile(this.filename, data)
    	})

    	await this.lock

    	function toLine({exists, ...e}: Deletable<Entity>): string {
    		return `${exists ? `E`: `D`}${JSON.stringify(e)}`
    	}
    }

    // QUERY OPERATORS

		private filter(query: Query<Entity>, entities: Entity[]): Entity[] {
			const filters = [this.filterAnd, this.filterOr, this.filterText, this.filterParams]

			return filters.reduce((acc, f) => f.call(this, query, acc), entities)
		}

		private filterAnd({$and}: Query<Entity>, entities: Entity[]): Entity[] {
			if (!!$and) {
				return $and.reduce((acc, sub) => this.filterParams(sub, acc), entities)
			}
			return entities
		}

		private filterOr({$or}: Query<Entity>, entities: Entity[]): Entity[] {
			if (!!$or) {
				return entities.filter((entity) => !!$or.find((sub) => this.queryMatches(sub, entity)))
			}
			return entities
		}

		private filterText({$text}: Query<Entity>, entities: Entity[]): Entity[] {
			if (!!$text) {
				const words = $text.toLowerCase().split(' ')
				return entities.filter((entity) => {
					return words.every((expected) => {
						return this.fullTextSearchFieldNames.some((f) => {
							const actual = (entity as any)[f].toLowerCase()
							return actual.match(new RegExp(`\\b${expected}\\b`))
						})
					})
				})
			}
			return entities
		}

		private filterParams(query: Query<Entity>, entities: Entity[]): Entity[] {
			return entities.filter(entity => this.queryMatches(query, entity))
		}

		private queryMatches(query: Query<Entity>, entity: Entity): boolean {
			return Object.entries(query as {[i: string]: QueryParam<$Values<Entity>>}).reduce((acc, [field, test]) => {
				if (!acc || field.startsWith('$')) {
					return acc
				}

				const [[matcher, expected]] = Object.entries(test)
				const actual = entity[field as $Keys<Entity>]
				switch (matcher) {
					case '$eq': return actual === expected
					case '$gt': return actual > expected
					case '$lt': return actual < expected
					case '$in': return expected.includes(actual)
				}
			}, true)
		}

		// QUERY OPTIONS

		private sort(sort: QuerySort<Entity> | undefined, entities: Entity[]): Entity[] {
			if (sort === undefined) return entities

			return entities.sort((a, b) => {
				return Object.entries(sort as {[i: string]: 1 | -1}).reduce((acc, [field, dir]) => {
					const aValue = a[field as $Keys<Entity>]
					const bValue = b[field as $Keys<Entity>]

					if (acc !== 0 || aValue === bValue) return acc
					if (aValue > bValue) return dir
					if (aValue < bValue) return -dir
					return 0
				}, 0)
			})
		}

		private project(projection: QueryProjection<Entity> | undefined, entities: Entity[]): Partial<Entity>[] {
			if (projection === undefined) return entities

			return entities.map((entity) => {
				return Object.keys(projection).reduce((acc, key) => ({
					...acc,
					[key]: entity[key as $Keys<Entity>],
				}), {})
			})
		}
}