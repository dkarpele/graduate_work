# Sprint 10, Notifications

### Installation

1. Clone [repo](https://github.com/dkarpele/notifications_sprint_1).
2. Create ```.env``` file according to ```.env.example```.
3. Launch the project ```docker-compose up --build```.


#### [architecture](architecture)

Architecture for the notifications service described here.

#### [notifications](notifications)

We use RabbitMQ as a broker and Sendgrid as an SMTP client. 

**Notify email**

- POST http://127.0.0.1/api/v1/notify-email/user-sign-up - send email notification to user after signing up

**Scheduler**

- likes_for_reviews. We send users an aggregated information about likes they received for their reviews to movies during last 24 hours. 

**Sendgrid**

With the help from Sendgrid we are sending individual and multiple emails. 
Sendgrid Marketing panel is used to send multiple emails to users from content manager. 

**Postgres**

After every action the transition status in the database changes:

Initiated -> Produced -> Consumed -> Sent.

- Initiated - message created
- Produced - message has been sent to queue
- Consumed - message has been received from queue
- Sent - message was sent to user (now by SMTP using Sendgrid)