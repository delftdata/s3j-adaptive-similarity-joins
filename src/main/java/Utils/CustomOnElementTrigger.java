package Utils;

import CustomDataTypes.FinalTuple;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class CustomOnElementTrigger extends Trigger<FinalTuple, GlobalWindow> {

    @Override
    public TriggerResult onElement(FinalTuple t, long l, GlobalWindow window, TriggerContext triggerContext) throws Exception {
        return TriggerResult.FIRE;
    }

    @Override
    public TriggerResult onProcessingTime(long l, GlobalWindow window, TriggerContext triggerContext) throws Exception {
        return null;
    }

    @Override
    public TriggerResult onEventTime(long l, GlobalWindow window, TriggerContext triggerContext) throws Exception {
        return null;
    }

    @Override
    public void clear(GlobalWindow window, TriggerContext triggerContext) throws Exception {

    }
}