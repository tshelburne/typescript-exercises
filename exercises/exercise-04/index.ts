import chalk from 'chalk';

/*

Intro:

    Time to filter the data! In order to be flexible
    we filter users using a number of criteria and
    return only those matching all of the criteria.
    We don't need Admins yet, we only filter Users.

Exercise:

    Without duplicating type structures, modify
    filterUsers function definition so that we can
    pass only those criteria which are needed,
    and not the whole User information as it is
    required now according to typing.

Higher difficulty bonus exercise:

    Exclude "type" from filter criterias.

Run:

    npm run 4

    - OR -

    yarn -s 4

*/

interface User {
    name: string;
    age: number;
    occupation: string;
}

interface Admin {
    name: string;
    age: number;
    role: string;
}

type Person = User | Admin;

const persons: Person[] = [
    { name: 'Max Mustermann', age: 25, occupation: 'Chimney sweep' },
    {
        name: 'Jane Doe',
        age: 32,
        role: 'Administrator'
    },
    {
        name: 'Kate Müller',
        age: 23,
        occupation: 'Astronaut'
    },
    {
        name: 'Bruce Willis',
        age: 64,
        role: 'World saver'
    },
    {
        name: 'Wilson',
        age: 23,
        occupation: 'Ball'
    },
    {
        name: 'Agent Smith',
        age: 23,
        role: 'Administrator'
    }
];

const isAdmin = (person: Person): person is Admin => !!(person as Admin).role;
const isUser = (person: Person): person is User => !!(person as User).occupation;

function logPerson(person: Person) {
    let additionalInformation: string = '';
    if (isAdmin(person)) {
        additionalInformation = person.role;
    }
    if (isUser(person)) {
        additionalInformation = person.occupation;
    }
    console.log(` - ${chalk.green(person.name)}, ${person.age}, ${additionalInformation}`);
}

function filterUsers(persons: Person[], criteria: Partial<User>): User[] {
    return persons.filter(isUser).filter((user) => {
        let criteriaKeys = Object.keys(criteria) as (keyof User)[];
        return criteriaKeys.every((fieldName) => {
            return user[fieldName] === criteria[fieldName];
        });
    });
}

console.log(chalk.yellow('Users of age 23:'));

filterUsers(
    persons,
    {
        age: 23
    }
).forEach(logPerson);

// In case if you are stuck:
// https://www.typescriptlang.org/docs/handbook/advanced-types.html#mapped-types
// https://www.typescriptlang.org/docs/handbook/release-notes/typescript-2-8.html#predefined-conditional-types
