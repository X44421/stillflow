import { useEffect, useState } from 'react';
import {
  Alert,
  Button,
  DescriptionList,
  DescriptionListDescription,
  DescriptionListGroup,
  DescriptionListTerm,
  DrawerActions,
  DrawerCloseButton,
  DrawerHead,
  DrawerPanelBody,
  ExpandableSection,
  Form,
  FormGroup,
  FormSelect,
  FormSelectOption,
  HelperText,
  HelperTextItem,
  Label,
  Progress,
  Switch,
  TextInput,
  Title,
} from '@patternfly/react-core';
import { BanIcon, PlayIcon, WrenchIcon } from '@patternfly/react-icons';

interface InspectorProps {
  objectTitle: string;
  objectType: string;
  isRunning: boolean;
  progress: number;
  error: boolean;
  onRunNode: () => void;
  onCancelRun: () => void;
  onValidate: () => void;
  onClose: () => void;
}

export function Inspector({
  objectTitle,
  objectType,
  isRunning,
  progress,
  error,
  onRunNode,
  onCancelRun,
  onValidate,
  onClose,
}: InspectorProps) {
  const [objectName, setObjectName] = useState(objectTitle);
  const [nodeType, setNodeType] = useState('transform');
  const [enabled, setEnabled] = useState(true);
  const [maxRecords, setMaxRecords] = useState('80000');
  const [retryPolicy, setRetryPolicy] = useState('3');
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [outputPath, setOutputPath] = useState('output/customer_clean.csv');
  const [mode, setMode] = useState('overwrite');
  const [schemaCheck, setSchemaCheck] = useState(true);

  useEffect(() => {
    setObjectName(objectTitle);
  }, [objectTitle]);

  return (
    <>
      <DrawerHead className="still-inspector-head">
        <div>
          <Title headingLevel="h2" size="lg">
            {objectTitle}
          </Title>
          <div className="still-inspector-head__meta">
            <Label color="blue" isCompact>
              {objectType}
            </Label>
            <Label color="grey" isCompact>
              Session
            </Label>
          </div>
        </div>
        <DrawerActions>
          <DrawerCloseButton onClose={onClose} />
        </DrawerActions>
      </DrawerHead>
      <DrawerPanelBody className="still-inspector-body">
        {error && (
          <Alert
            variant="danger"
            isInline
            title="Run failed"
            className="still-inspector-alert"
          >
            The selected node stopped before completing. Review the node settings and run again.
          </Alert>
        )}

        {isRunning && (
          <Progress
            value={progress}
            title="Execution progress"
            measureLocation="outside"
            aria-label="Execution progress"
          />
        )}

        <DescriptionList isHorizontal isCompact>
          <DescriptionListGroup>
            <DescriptionListTerm>Object</DescriptionListTerm>
            <DescriptionListDescription>{objectName}</DescriptionListDescription>
          </DescriptionListGroup>
          <DescriptionListGroup>
            <DescriptionListTerm>Rows</DescriptionListTerm>
            <DescriptionListDescription>80,000</DescriptionListDescription>
          </DescriptionListGroup>
          <DescriptionListGroup>
            <DescriptionListTerm>Last run</DescriptionListTerm>
            <DescriptionListDescription>{isRunning ? 'In progress' : '3 minutes ago'}</DescriptionListDescription>
          </DescriptionListGroup>
        </DescriptionList>

        <Form>
          <FormGroup label="Name" fieldId="inspector-name">
            <TextInput
              id="inspector-name"
              value={objectName}
              onChange={(_event, value) => setObjectName(value)}
              aria-label="Object name"
            />
            <HelperText>
              <HelperTextItem>Display name for the selected object.</HelperTextItem>
            </HelperText>
          </FormGroup>

          <FormGroup label="Type" fieldId="inspector-type">
            <FormSelect
              id="inspector-type"
              value={nodeType}
              onChange={(_event, value) => setNodeType(value)}
              aria-label="Node type"
            >
              <FormSelectOption value="source" label="Source" />
              <FormSelectOption value="transform" label="Transform" />
              <FormSelectOption value="dedup" label="Deduplication" />
              <FormSelectOption value="output" label="Output" />
            </FormSelect>
          </FormGroup>

          <FormGroup label="Maximum records" fieldId="inspector-records">
            <TextInput
              id="inspector-records"
              type="number"
              value={maxRecords}
              onChange={(_event, value) => setMaxRecords(value)}
              aria-label="Maximum records"
            />
          </FormGroup>

          <FormGroup label="Retry policy" fieldId="inspector-retry">
            <FormSelect
              id="inspector-retry"
              value={retryPolicy}
              onChange={(_event, value) => setRetryPolicy(value)}
              aria-label="Retry policy"
            >
              <FormSelectOption value="0" label="No retries" />
              <FormSelectOption value="3" label="3 retries" />
              <FormSelectOption value="5" label="5 retries" />
            </FormSelect>
          </FormGroup>

          <FormGroup label="Enabled" fieldId="inspector-enabled">
            <Switch
              id="inspector-enabled"
              label="Run this node"
              isChecked={enabled}
              onChange={(_event, checked) => setEnabled(checked)}
            />
          </FormGroup>

          <ExpandableSection
            toggleText="Advanced settings"
            isExpanded={advancedOpen}
            onToggle={(_event, isExpanded) => setAdvancedOpen(isExpanded)}
            isIndented
          >
            <FormGroup label="Output path" fieldId="inspector-path">
              <TextInput
                id="inspector-path"
                value={outputPath}
                onChange={(_event, value) => setOutputPath(value)}
                aria-label="Output path"
              />
            </FormGroup>
            <FormGroup label="Write mode" fieldId="inspector-mode">
              <FormSelect
                id="inspector-mode"
                value={mode}
                onChange={(_event, value) => setMode(value)}
                aria-label="Write mode"
              >
                <FormSelectOption value="overwrite" label="Overwrite" />
                <FormSelectOption value="append" label="Append" />
                <FormSelectOption value="error" label="Fail if exists" />
              </FormSelect>
            </FormGroup>
            <FormGroup label="Schema check" fieldId="inspector-schema">
              <Switch
                id="inspector-schema"
                label="Validate output schema"
                isChecked={schemaCheck}
                onChange={(_event, checked) => setSchemaCheck(checked)}
              />
            </FormGroup>
          </ExpandableSection>
        </Form>

        <div className="still-inspector-actions">
          {isRunning ? (
            <Button variant="danger" icon={<BanIcon />} onClick={onCancelRun}>
              Cancel run
            </Button>
          ) : (
            <Button variant="primary" icon={<PlayIcon />} onClick={onRunNode} isDisabled={!enabled}>
              Run node
            </Button>
          )}
          <Button variant="secondary" icon={<WrenchIcon />} onClick={onValidate} isDisabled={isRunning}>
            Validate
          </Button>
        </div>
      </DrawerPanelBody>
    </>
  );
}
