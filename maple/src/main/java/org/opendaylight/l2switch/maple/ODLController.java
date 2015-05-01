/**
 * Copyright (c) 2014 Cisco Systems, Inc. and others.  All rights reserved.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Eclipse Public License v1.0 which accompanies this distribution,
 * and is available at http://www.eclipse.org/legal/epl-v10.html
 */

package org.opendaylight.l2switch.maple;


import org.maple.core.Controller;
import org.maple.core.MapleSystem;
import org.maple.core.Rule;
import org.maple.core.ToPorts;
import org.maple.core.Drop;
import org.maple.core.Punt;
import org.maple.core.Action;
import org.maple.core.TraceItem;

import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorUpdatedBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.Nodes;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.match.EthernetMatchBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.ethernet.match.fields.EthernetDestinationBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.ethernet.match.fields.EthernetSourceBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.MatchBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.l2.types.rev130827.EtherType;
import org.opendaylight.yang.gen.v1.urn.opendaylight.model.match.types.rev131026.ethernet.match.fields.EthernetTypeBuilder;

import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.yang.types.rev100924.MacAddress;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.FlowCapableNode;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.flow.Match;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.Table;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.TableKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.Flow;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.inventory.rev130819.tables.table.FlowKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.FlowCookie;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeId;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnector;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.node.NodeConnectorKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.Node;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.nodes.NodeKey;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingListener;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketProcessingService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.PacketReceived;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.TransmitPacketInput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.packet.service.rev130709.TransmitPacketInputBuilder;
import org.opendaylight.yangtools.yang.binding.InstanceIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.SalFlowService;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.AddFlowOutput;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.AddFlowInputBuilder;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.types.rev131026.FlowRef;
import org.opendaylight.yang.gen.v1.urn.opendaylight.flow.service.rev130819.FlowTableRef;
import org.opendaylight.yang.gen.v1.urn.ietf.params.xml.ns.yang.ietf.inet.types.rev100924.Uri;
import java.util.concurrent.Future;
import org.opendaylight.yangtools.yang.common.RpcResult;

import org.opendaylight.yangtools.yang.binding.DataObject;
import org.opendaylight.controller.md.sal.binding.api.DataChangeListener;
import org.opendaylight.controller.md.sal.common.api.data.AsyncDataChangeEvent;

import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorRemoved;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeConnectorUpdated;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeRemoved;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.NodeUpdated;
import org.opendaylight.yang.gen.v1.urn.opendaylight.inventory.rev130819.OpendaylightInventoryListener;

public class ODLController implements DataChangeListener,
                                      OpendaylightInventoryListener,
                                      PacketProcessingListener,
                                      Controller {

  protected static final Logger LOG = LoggerFactory.getLogger(ODLController.class);

  private MapleSystem maple;

  private FlowCommitWrapper dataStoreAccessor;

  private NodeId nodeId;
  private short flowTableId = 0;
  private AtomicLong flowIdInc = new AtomicLong();

  private InstanceIdentifier<Node> nodePath;
  private InstanceIdentifier<Table> tablePath;

  private Map<Integer, NodeConnectorRef> portToNodeConnectorRef;
  private Map<Integer, MacAddress> portToMacAddress;

  private static final String LOCAL_PORT_STR = "LOCAL";

  /* Given from Activator. */

  private PacketProcessingService pps;
  private SalFlowService fs;

  private ODLController() {}

  public ODLController(PacketProcessingService pps, SalFlowService fs) {
    this.pps = pps;
    this.fs = fs;
  }

  /* Implements DataChangeListener. */

  public void setDataStoreAccessor(FlowCommitWrapper dataStoreAccessor) {
    this.dataStoreAccessor = dataStoreAccessor;
  }

  /* Implements OpendaylightInventoryListener. */

  @Override
  public void onNodeConnectorRemoved(NodeConnectorRemoved notification) {
    NodeConnectorRef ncr = notification.getNodeConnectorRef();
    String portID = ncr
      .getValue()
      .firstIdentifierOf(NodeConnector.class)
      .firstKeyOf(NodeConnector.class, NodeConnectorKey.class)
      .getId()
      .getValue();

    if (portID.contains(LOCAL_PORT_STR))
      return;

    int portNum = portStrToInt(portID);
    this.portToNodeConnectorRef.remove(portNum);
    this.maple.portDown(portNum);

    System.out.println("NodeConnectorRef " + notification.getNodeConnectorRef());
  }

  @Override
  public void onNodeConnectorUpdated(NodeConnectorUpdated notification) {
    NodeConnectorRef ncr = notification.getNodeConnectorRef();
    String portID = ncr
      .getValue()
      .firstIdentifierOf(NodeConnector.class)
      .firstKeyOf(NodeConnector.class, NodeConnectorKey.class)
      .getId()
      .getValue();

    if (portID.contains(LOCAL_PORT_STR))
      return;

    int portNum = portStrToInt(portID);
    this.portToNodeConnectorRef.put(portNum, ncr);
    this.maple.portUp(portNum);

    System.out.println("NodeConnectorRef " + notification.getNodeConnectorRef());
  }

  @Override
  public void onNodeRemoved(NodeRemoved notification) {
    System.out.println("NodeRef " + notification.getNodeRef());
  }

  @Override
  public void onNodeUpdated(NodeUpdated notification) {
    System.out.println("NodeRef " + notification.getNodeRef());
  }

  @Override
  public void onPacketReceived(PacketReceived packet) {
    if (packet == null || packet.getPayload() ==  null)
      return;

    byte[] data = packet.getPayload();

    LOG.debug("Received packet via match: {}", packet.getMatch());

    // read src MAC and dst MAC
    byte[] dstMacRaw = PacketUtils.extractDstMac(packet.getPayload());
    byte[] srcMacRaw = PacketUtils.extractSrcMac(packet.getPayload());
    byte[] etherType = PacketUtils.extractEtherType(packet.getPayload());

    NodeConnectorRef ingress = packet.getIngress();

    if (ingress == null)
      return;

    String portID = ingress
      .getValue()
      .firstIdentifierOf(NodeConnector.class)
      .firstKeyOf(NodeConnector.class, NodeConnectorKey.class)
      .getId()
      .getValue();

    int switchNum = switchStrToInt(portID);
    int portNum = portStrToInt(portID);

    MacAddress dstMac = PacketUtils.rawMacToMac(dstMacRaw);
    MacAddress srcMac = PacketUtils.rawMacToMac(srcMacRaw);

    this.portToMacAddress.put(portNum, srcMac);

    System.out.println("Mapping portNum "+portNum+" to NodeConnectorRef. ");
    synchronized(this) {
      this.maple.handlePacket(data, switchNum, portNum);
    }
  }

  private static int switchStrToInt(String switchStr) {
    return Integer.parseInt(switchStr.substring(
      switchStr.indexOf(':') + 1, switchStr.lastIndexOf(':')));
  }

  private static int portStrToInt(String portStr) {
    return Integer.parseInt(portStr.substring(
      portStr.lastIndexOf(':') + 1));
  }

  /**
   * starting controller
   */
  public void start() {
    LOG.debug("start() -->");

    this.maple = new MapleSystem(this);
    System.out.println("Maple Initiated");
    this.portToNodeConnectorRef = new HashMap<>();
    this.portToMacAddress = new HashMap<>();

    this.nodePath = InstanceIdentifierUtils.createNodePath(new NodeId("node_001"));

    LOG.debug("start() <--");
  }

  /**
   * stopping controller
   */
  public void stop() {
    LOG.debug("stop() -->");
    //TODO: remove flow (created in #start())
    LOG.debug("stop() <--");
  }
 
  private NodeConnectorRef ingressPlaceHolder(int portNum) {
    if (this.portToNodeConnectorRef.containsKey(portNum))
      return this.portToNodeConnectorRef.get(portNum);
    else
      throw new IllegalArgumentException("portNum " + portNum + " does not exist in map");
  }

  public synchronized Future<RpcResult<AddFlowOutput>> onSwitchAppeared(InstanceIdentifier<Table> appearedTablePath) {

    LOG.debug("expected table acquired, learning ..");

    tablePath = appearedTablePath;
    nodePath = tablePath.firstIdentifierOf(Node.class);
    nodeId = nodePath.firstKeyOf(Node.class, NodeKey.class).getId();

    return null;
  }

  @Override
  public void onDataChanged(AsyncDataChangeEvent<InstanceIdentifier<?>, DataObject> change) {
    Short requiredTableId = 0;

    Map<InstanceIdentifier<?>, DataObject> updated = change.getUpdatedData();
    for (Entry<InstanceIdentifier<?>, DataObject> updateItem : updated.entrySet()) {
      DataObject table = updateItem.getValue();
      if (table instanceof Table) {
        Table tableSure = (Table) table;
        LOG.trace("table: {}", table);

        if (requiredTableId.equals(tableSure.getId())) {
          @SuppressWarnings("unchecked")
          InstanceIdentifier<Table> tablePath = (InstanceIdentifier<Table>) updateItem.getKey();
          onSwitchAppeared(tablePath);
        }
      }
    }
  }

  public void sendPacket(byte[] data, int inSwitch, int inPort, int... ports) {
    System.out.println("sendPacket Called in handler");

    if (ports[0] == Integer.MAX_VALUE) {
      flood(data, ingressPlaceHolder(inPort));
      return;
    }

    for (int i = 0; i < ports.length; i++) {
      NodeConnectorRef ncRef = PacketUtils.createNodeConnRef(
        nodePath,
        nodePath.firstKeyOf(Node.class, NodeKey.class),
        ports[i] + "");
      sendPacketOut(data, ingressPlaceHolder(inPort), ncRef);
    }

  }

  private Future<RpcResult<AddFlowOutput>> writeFlowToController(InstanceIdentifier<Node> nodeInstanceId,
                                                                 InstanceIdentifier<Table> tableInstanceId,
                                                                 InstanceIdentifier<Flow> flowPath,
                                                                 Flow flow) {
    final AddFlowInputBuilder builder = new AddFlowInputBuilder(flow);
    builder.setNode(new NodeRef(nodeInstanceId));
    builder.setFlowRef(new FlowRef(flowPath));
    builder.setFlowTable(new FlowTableRef(tableInstanceId));
    builder.setTransactionUri(new Uri(flow.getId().getValue()));
    return fs.addFlow(builder.build());
  }

  private InstanceIdentifier<Table> getTableInstanceId(InstanceIdentifier<Node> nodeId) {
    // get flow table key
    TableKey flowTableKey = new TableKey(flowTableId);

    return nodeId.builder()
        .augmentation(FlowCapableNode.class)
        .child(Table.class, flowTableKey)
        .build();
  }

  private InstanceIdentifier<Flow> getFlowInstanceId(InstanceIdentifier<Table> tableId) {
    // generate unique flow key
    FlowId flowId = new FlowId(String.valueOf(flowIdInc.getAndIncrement()));
    FlowKey flowKey = new FlowKey(flowId);
    return tableId.child(Flow.class, flowKey);
  }

  private void installDropRule(Rule rule, int outSwitch) {
    /* TODO: add match. */
    InstanceIdentifier<Table> tableId = getTableInstanceId(this.nodePath);
    InstanceIdentifier<Flow> flowId = getFlowInstanceId(tableId);

    Future<RpcResult<AddFlowOutput>> result;
    result = writeFlowToController(this.nodePath, tableId, flowId,
      FlowUtils.createDropAllFlow(this.flowTableId, rule.priority).build());
  }

  private void installPuntRule(Rule rule, int outSwitch) {
     /* TODO: add match. */
    InstanceIdentifier<Table> tableId = getTableInstanceId(this.nodePath);
    InstanceIdentifier<Flow> flowId = getFlowInstanceId(tableId);

    Future<RpcResult<AddFlowOutput>> result;
    result = writeFlowToController(this.nodePath, tableId, flowId,
      FlowUtils.createPuntAllFlow(this.flowTableId, rule.priority).build());
  }

  public Match matchForRule(Rule rule) {
    MatchBuilder matchBuilder = new MatchBuilder();
    EthernetMatchBuilder ethernetMatchBuilder = new EthernetMatchBuilder();
    MacAddress addr;
    for (TraceItem item : rule.match.fieldValues) {
      switch (item.field) {
        case IN_PORT:
          matchBuilder.setInPort(nodeConnectorId(Long.toString(item.value)));
          break;
        case ETH_SRC:
          addr = PacketUtils.macValueToMac(item.value);
          ethernetMatchBuilder.setEthernetSource(
              new EthernetSourceBuilder()
              .setAddress(addr)
              .build());
          break;
        case ETH_DST:
          addr = PacketUtils.macValueToMac(item.value);
          ethernetMatchBuilder.setEthernetDestination(
              new EthernetDestinationBuilder()
              .setAddress(addr)
              .build());
          break;
        case ETH_TYPE:
          ethernetMatchBuilder.setEthernetType(
              new EthernetTypeBuilder()
              .setType(new EtherType(item.value))
              .build());
          break;
        default:
          assert false;
          break;
      }
    }
    matchBuilder.setEthernetMatch(ethernetMatchBuilder.build());
    Match m = matchBuilder.build();
    return m;
  }

  private void installToPortRule(Rule rule, int outSwitch, int[] outPorts) {

    NodeConnectorRef dstPorts[] = new NodeConnectorRef[outPorts.length];
    for (int i = 0; i < outPorts.length; i++) {
      dstPorts[i] = this.portToNodeConnectorRef.get(outPorts[i]);
      if (dstPorts[i] == null) {
        System.out.println("!!!!!!!! WARNING - NOT INSTALLING RULE: " + rule + "!!!!!!!!!!!!!!");
        return;
      }
    }

    // System.out.println("Installing toPort rule"+rule.toString());
    InstanceIdentifier<Table> tableId = getTableInstanceId(this.nodePath);
    InstanceIdentifier<Flow> flowId = getFlowInstanceId(tableId);

    Future<RpcResult<AddFlowOutput>> result;
    result = writeFlowToController(this.nodePath, tableId, flowId,
      FlowUtils.createToPortFlow(this.flowTableId,
                                 rule.priority,
                                 matchForRule(rule),
                                 dstPorts).build());
  }

  public void installRules(LinkedList<Rule> rules, int... outSwitches) {
    for (Rule rule : rules) {
      Action a = rule.action;
      if (a instanceof ToPorts) {
        int[] outPorts = ((ToPorts)a).portIDs;
        for (int i = 0; i < outSwitches.length; i++) {
          installToPortRule(rule, outSwitches[i], outPorts);
        }
      } else {
        throw new IllegalArgumentException("unknown rule type: " + rule);
      }
      /*
      for (Action action : rule.actions) {

        if (false) { //action instanceof Drop) {
          for (int i = 0; i < outSwitches.length; i++)
            installDropRule(rule, outSwitches[i]);

        } else if (false) { //action instanceof Punt) {
          for (int i = 0; i < outSwitches.length; i++)
            installPuntRule(rule, outSwitches[i]);

        } else if (action instanceof ToPort) {
          int outPort = ((ToPort)action).portID;
          for (int i = 0; i < outSwitches.length; i++)
            installToPortRule(rule, outSwitches[i], outPort);

        } else {
          throw new IllegalArgumentException("unknown rule type: " + rule);
        }
      }
      */
    }
  }

  private void flood(byte[] payload, NodeConnectorRef ingress) {
    NodeConnectorKey nodeConnectorKey = new NodeConnectorKey(nodeConnectorId("0xfffffffb"));
    InstanceIdentifier<?> nodeConnectorPath = InstanceIdentifierUtils.createNodeConnectorPath(nodePath, nodeConnectorKey);
    NodeConnectorRef egressConnectorRef = new NodeConnectorRef(nodeConnectorPath);

    sendPacketOut(payload, ingress, egressConnectorRef);
  }

  private NodeConnectorId nodeConnectorId(String connectorId) {
    NodeKey nodeKey = nodePath.firstKeyOf(Node.class, NodeKey.class);
    StringBuilder stringId = new StringBuilder(nodeKey.getId().getValue()).append(":").append(connectorId);
    return new NodeConnectorId(stringId.toString());
  }

  private void sendPacketOut(byte[] payload, NodeConnectorRef ingress, NodeConnectorRef egress) {
    InstanceIdentifier<Node> egressNodePath = InstanceIdentifierUtils.getNodePath(egress.getValue());
    TransmitPacketInput input = new TransmitPacketInputBuilder()
      .setPayload(payload)
      .setNode(new NodeRef(egressNodePath))
      .setEgress(egress)
      .setIngress(ingress)
      .build();
    pps.transmitPacket(input);
  }

}
