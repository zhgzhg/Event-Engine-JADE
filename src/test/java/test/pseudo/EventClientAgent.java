package test.pseudo;

import jade.core.Agent;
import jade.core.behaviours.ThreadedBehaviourFactory;
import jade.core.behaviours.WakerBehaviour;
import net.uniplovdiv.fmi.cs.vrs.event.Event;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.behaviour.BEventBrokerSubscriber;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.behaviour.BEventChannel;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.ontology.SubscriptionParameter;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.util.ServiceDescriptionUtils;
import net.uniplovdiv.fmi.cs.vrs.jade.agent.util.YellowPagesUtils;

/**
 * Sample, simple client agent to test event broker agents. Not a real unit test, yet...
 */
public class EventClientAgent extends Agent {
    private static final long serialVersionUID = 3538576756834034664L;
    private ThreadedBehaviourFactory tbf = new ThreadedBehaviourFactory();
    private SubscriptionParameter sp = new SubscriptionParameter();

    @Override
    protected void setup() {
        addBehaviour(new WakerBehaviour(this, 20000) {
            private static final long serialVersionUID = 654432216612738021L;

            @Override
            protected void onWake() {
                BEventBrokerSubscriber bbs = new BEventBrokerSubscriber(new YellowPagesUtils(getAgent(),
                        ServiceDescriptionUtils.createEventSourceSD(null)), null);
                addBehaviour(tbf.wrap(bbs));

                BEventChannel bes = new BEventChannel(getAgent(), 300, bbs, (event) ->
                        System.out.println(getAID() + ": Received " + event)
                );
                addBehaviour(bes);

                Event event = Event.makeInstance(Event.class);
                event.getDynamicParameters().put("sender-aid", getAID().toString());
                bes.send(event);
            }
        });
    }

    @Override
    protected void takeDown() {
        try {
            ((BEventBrokerSubscriber) this.tbf.getWrappers()[0].getBehaviour()).setDone(true);
            this.tbf.getWrappers()[0].getBehaviour().action();
        } catch (NullPointerException e) {
            // we don't care at this point
        }
        this.tbf.interrupt();
    }
}
