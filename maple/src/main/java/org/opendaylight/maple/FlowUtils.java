package org.opendaylight.maple;

import java.util.ArrayList;
import java.util.List;

import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.Match;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.FlowCookie;
import java.util.concurrent.atomic.AtomicLong;
import java.math.BigInteger;

import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.inet.types.rev100924.Uri;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.OutputActionCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.DropActionCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.action.output.action._case.OutputActionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.list.Action;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.list.ActionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.action.types.rev131112.action.list.ActionKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.FlowModFlags;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.OutputPortValues;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.InstructionsBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.MatchBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.instruction.ApplyActionsCaseBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.instruction.apply.actions._case.ApplyActions;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.instruction.apply.actions._case.ApplyActionsBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.list.Instruction;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.list.InstructionBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.instruction.list.InstructionKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnector;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnectorKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.ethernet.match.fields.EthernetDestinationBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.ethernet.match.fields.EthernetSourceBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.match.EthernetMatch;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.match.EthernetMatchBuilder;

import com.google.common.collect.ImmutableList;

public class FlowUtils {

  private FlowUtils() { }

  private static AtomicLong flowCookieInc = new AtomicLong(0x2a00000000000000L);

  /**
   * @param tableId
   * @param priority
   * @return {@link FlowBuilder} forwarding all packets to controller port
   */
  public static FlowBuilder createPuntFlow(Short tableId, int priority, Match match) {
    FlowBuilder puntAll = new FlowBuilder()
      .setTableId(tableId)
      .setFlowName("puntall");

    puntAll.setId(new FlowId(Long.toString(puntAll.hashCode())));

    OutputActionBuilder output = new OutputActionBuilder();
    output.setMaxLength(Integer.valueOf(0xffff));
    Uri controllerPort = new Uri(OutputPortValues.CONTROLLER.toString());
    output.setOutputNodeConnector(controllerPort);

    Action puntAllAction = new ActionBuilder()
      .setOrder(0)
      .setAction(new OutputActionCaseBuilder()
        .setOutputAction(output.build()).build())
      .build();

    ApplyActions applyActions = new ApplyActionsBuilder()
      .setAction(ImmutableList.of(puntAllAction))
      .build();

    Instruction applyActionsInstruction = new InstructionBuilder()
      .setOrder(0)
      .setInstruction(new ApplyActionsCaseBuilder()
         .setApplyActions(applyActions)
         .build())
      .build();

    puntAll
      .setMatch(match)
      .setInstructions(new InstructionsBuilder()
         .setInstruction(ImmutableList.of(applyActionsInstruction))
         .build())
      .setPriority(priority)
      .setBufferId(0xffffffffL)
      .setHardTimeout(0)
      .setIdleTimeout(0)
      .setCookie(new FlowCookie(BigInteger.valueOf(flowCookieInc.getAndIncrement())))
      .setFlags(new FlowModFlags(false, false, false, false, false));

    return puntAll;
  }

  /**
   * @param tableId
   * @param priority
   * @param match
   * @param dstPort
   * @return {@link FlowBuilder} forwarding all packets to output port
   */
  public static FlowBuilder
  createToPortFlow(Short tableId,
                   int priority,
                   Match match, 
                   NodeConnectorRef[] dstPorts) {

    FlowBuilder toPort = new FlowBuilder()
      .setTableId(tableId)
      .setFlowName("toPort");

    toPort.setId(new FlowId(Long.toString(toPort.hashCode())));

    ArrayList<Action> outputActions = new ArrayList<Action>();
    for (NodeConnectorRef dstPort : dstPorts) {
      Uri outputPort = dstPort.getValue().firstKeyOf(NodeConnector.class, NodeConnectorKey.class).getId();
      Action toPortAction = new ActionBuilder()
          .setOrder(0)
          .setAction(new OutputActionCaseBuilder()
                     .setOutputAction(new OutputActionBuilder()
                                      .setMaxLength(Integer.valueOf(0xffff))
                                      .setOutputNodeConnector(outputPort)
                                      .build())
                     .build())
          .build();
      outputActions.add(toPortAction);
    }
    ApplyActions applyActions =
        new ApplyActionsBuilder()
        .setAction(outputActions)
        .build();

    Instruction applyActionsInstruction = new InstructionBuilder()
      .setOrder(0)
      .setInstruction(new ApplyActionsCaseBuilder()
      .setApplyActions(applyActions)
        .build())
      .build();

    toPort
      .setMatch(match)
      .setInstructions(new InstructionsBuilder()
        .setInstruction(ImmutableList.of(applyActionsInstruction))
        .build())
      .setPriority(priority)
      .setBufferId(0xffffffffL)
      .setHardTimeout(0)
      .setIdleTimeout(0)
      .setCookie(new FlowCookie(BigInteger.valueOf(flowCookieInc.getAndIncrement())))
      .setFlags(new FlowModFlags(false, false, false, false, false));

    return toPort;
  }
}
