# kafka_ruleengine


Reads JSON formatted data from a Kafka topic, runs business rules (logic) on the data and
outputs the resulting data to a Kafka target topic.
 
Optionally the detailed results of the execution of the ruleengine may be ouput to a defined
topic for logging purposes. In this case the topic will contain one output message for each
input message and rule. E.g. if there are 10 rules in the ruleengine project file, then for any
given input message, 10 ouput messages are generated. The output message also contains details
which rules failed, which rulegroups failed and the related rule message.

The source topic data is expected to be in JSON format. Output will be in JSON format.

Please send your feedback and help to enhance the tool.

    Copyright (C) 2006-2018  Uwe Geercken


 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.


uwe geercken
uwe.geercken@web.de

last update: 2018-05-30

