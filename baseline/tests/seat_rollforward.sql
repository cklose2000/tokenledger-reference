select month,plan from {{ref('subscriptions')}}
where paid_seats<>beginning_seats+gross_adds-cancelled_seats-seat_removals+plan_transfers_net
