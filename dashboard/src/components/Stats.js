      {event && event.max_buy_price && <p>Max Buy Price: ${event.max_buy_price}</p>}import { useEffect, useState } from 'react'

const Stats = () => {
    const [event, setEvent] = useState([])

    useEffect(() => {
        fetch('http://52.38.236.128/processing/stats')
        .then(res => res.json())
        .then(res => { 
            setEvent(res) 
        })
    }, [])

console.log(event)

    return (
        <div className="stats">
            <h2>Latest Statistics</h2>
            <div>
                <p>Max Buy Price: ${event.max_buy_price}</p>
		<p>Max Sell Price: ${event.max_sell_price}</p>
		<p>Number of purchases: {event.num_buys}</p>
		<p>Number of sales: {event.num_sells}</p>
                {/* output stats here */}
            </div>
        </div>
    )   
}

export default Stats
