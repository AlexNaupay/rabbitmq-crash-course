'use strict'

const amqp = require('amqplib')
const queue = process.env.QUEUE || 'hello'
const exchangeName = process.env.EXCHANGE || 'my-direct'
const exchangeType = 'direct'
const pattern = process.env.PATTERN || ''
let counter = 1;
const rejector = process.env.REJECTOR || false;

console.log({
    queue,
    exchangeName,
    pattern
})

function intensiveOperation() {
    let i = 1e3
    while (i--) {}
}

async function subscriber() {
    const connection = await amqp.connect('amqp://localhost')
    const channel = await connection.createChannel()

    await channel.assertQueue(queue)

    await channel.assertExchange(exchangeName, exchangeType)

    await channel.bindQueue(queue, exchangeName, pattern)

    channel.consume(queue, (message) => {
        const content = JSON.parse(message.content.toString())

        intensiveOperation()

        console.log(`Received message from "${queue}" queue = `, content)

        if (rejector && (counter === 1 || counter > 3)){
            setTimeout(()=>{
                console.log(`Rejected: ${content.id}`)
                channel.nack(message, false)
            }, 2500)

        }else {
            channel.ack(message)
            console.log(`Processed: ${content.id}`)
        }

        counter++
    })
}

subscriber().catch((error) => {
    console.error(error)
    process.exit(1)
})


// Misma cola balanceo
// Diferente cola, cada uno recibe todos