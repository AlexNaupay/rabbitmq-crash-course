'use strict'

const amqp = require('amqplib')
const queue = process.env.QUEUE || 'hello'
let counter = 1;
const rejector = process.env.REJECTOR || false;

function intensiveOperation() {
    let i = 1e3
    while (i--) {}
}

async function subscriber() {
    const connection = await amqp.connect('amqp://localhost')
    const channel = await connection.createChannel()

    await channel.assertQueue(queue)

    channel.consume(queue, (message) => {
        const content = JSON.parse(message.content.toString())

        intensiveOperation()

        console.log(`Received message from "${queue}" queue = `, content)

        if (counter === 1 && rejector){
            setTimeout(()=>{
                console.log(`Rejected: ${content.id}`)
                channel.nack(message, false)
            }, 3500)

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
